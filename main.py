import json
import logging
import os
import pymongo
import datetime
import time
import concurrent.futures
import queue
import multiprocessing
import threading
from multiprocessing import Process, Queue, JoinableQueue, Event
from functools import partial

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Counter, Gauge, Summary, multiprocess, CollectorRegistry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration variables
IS_AUTHENTICATE_KAFKA = int(os.getenv("IS_AUTHENTICATE_KAFKA", "1"))
JSON_FIELDS = os.getenv("JSON_FIELDS", "attach_field").split(",")
DATETIME_FIELDS = os.getenv("DATETIME_FIELDS", "created_at,updated_at").split(",")
NUM_WORKER_PROCESSES = int(os.getenv("NUM_WORKER_PROCESSES", "2"))  # Number of worker processes
NUM_THREADS_PER_PROCESS = int(os.getenv("NUM_THREADS_PER_PROCESS", "2"))  # Threads per process
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "1000"))  # Max queue size
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Messages to process in a batch
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "1000"))  # Max wait time for batch
COMMIT_INTERVAL_MS = int(os.getenv("COMMIT_INTERVAL_MS", "5000"))  # Offset commit interval
microseconds = 1000000

# Set up multiprocess metrics directory
os.environ["PROMETHEUS_MULTIPROC_DIR"] = os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc")
if not os.path.exists(os.environ["PROMETHEUS_MULTIPROC_DIR"]):
    os.makedirs(os.environ["PROMETHEUS_MULTIPROC_DIR"], exist_ok=True)

# Create registry for multiprocess metrics
registry = CollectorRegistry()

# Prometheus metrics
CDC_EVENTS_PROCESSED = Counter('cdc_events_processed_total', 'Total CDC events processed', registry=registry)
CDC_EVENT_LATENCY = Summary('cdc_event_processing_latency_seconds', 'Processing latency', registry=registry)
CDC_ERRORS = Counter('cdc_errors_total', 'Total errors encountered', registry=registry)
CDC_SKIPPED_EVENTS = Counter('cdc_skipped_events_total', 'Total CDC events skipped', registry=registry)
CDC_CURRENT_LAG = Gauge('cdc_current_lag_seconds', 'Lag between CDC event generation and processing', registry=registry)
CDC_EVENT_TYPES = Counter('cdc_event_types_total', 'CDC events by type', ['event_type', 'process_id'], registry=registry)
CDC_QUEUE_SIZE = Gauge('cdc_queue_size', 'Current size of processing queue', ['queue_name'], registry=registry)
CDC_BATCH_SIZE = Summary('cdc_batch_size', 'Size of processed batches', registry=registry)
CDC_BATCH_PROCESSING_TIME = Summary('cdc_batch_processing_time_seconds', 'Batch processing time', registry=registry)

class MessageProcessor:
    """Class to manage message processing with worker threads."""
    
    def __init__(self, process_id, message_queue, result_queue, shutdown_event):
        self.process_id = process_id
        self.message_queue = message_queue
        self.result_queue = result_queue
        self.shutdown_event = shutdown_event
        self.worker_threads = []
        
        # MongoDB connection for this process
        self.mongo_client = pymongo.MongoClient(
            os.getenv("MONGO_URI"),
            maxPoolSize=NUM_THREADS_PER_PROCESS + 2,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000
        )
        self.db = self.mongo_client.get_database(os.getenv("MONGO_DB"))
        self.collection = self.db.get_collection(os.getenv("MONGO_COLLECTION"))
        
        # Local processing queue
        self.processing_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        
        logger.info(f"Process {self.process_id} initialized with {NUM_THREADS_PER_PROCESS} threads")

    def start(self):
        """Start worker threads."""
        # Start thread to get messages from inter-process queue
        self.queue_handler = threading.Thread(target=self._handle_message_queue)
        self.queue_handler.daemon = True
        self.queue_handler.start()
        
        # Start worker threads
        for i in range(NUM_THREADS_PER_PROCESS):
            worker = threading.Thread(target=self._worker_thread, name=f"process-{self.process_id}-thread-{i}")
            worker.daemon = True
            worker.start()
            self.worker_threads.append(worker)
            
    def shutdown(self):
        """Shutdown processor and its threads."""
        logger.info(f"Process {self.process_id} shutting down")
        
        # Signal worker threads to terminate
        for _ in self.worker_threads:
            self.processing_queue.put(None)
            
        # Wait for threads to complete
        for worker in self.worker_threads:
            worker.join(timeout=5.0)
            
        self.mongo_client.close()
        logger.info(f"Process {self.process_id} shutdown complete")
        
    def _handle_message_queue(self):
        """Thread that transfers messages from inter-process queue to local queue."""
        while not self.shutdown_event.is_set():
            try:
                # Get message from inter-process queue
                message = self.message_queue.get(timeout=0.5)
                if message is None:  # Sentinel for shutdown
                    self.message_queue.task_done()
                    break
                    
                # Put in local processing queue
                self.processing_queue.put(message)
                self.message_queue.task_done()
                
                # Update metrics
                CDC_QUEUE_SIZE.labels(queue_name=f"process_{self.process_id}_local").set(self.processing_queue.qsize())
                
            except (queue.Empty, EOFError):
                continue
            except Exception as e:
                logger.error(f"Process {self.process_id} queue handler error: {e}")
                
    def _worker_thread(self):
        """Worker thread that processes messages from the local queue."""
        thread_name = threading.current_thread().name
        logger.info(f"Started worker thread {thread_name}")
        batch = []
        last_process_time = time.time()
        
        while True:
            try:
                # Try to get message with timeout
                try:
                    message = self.processing_queue.get(timeout=0.1)
                    if message is None:  # Sentinel to shut down
                        self.processing_queue.task_done()
                        break
                        
                    result = self._process_message(message)
                    if result:
                        batch.append(result)
                        
                    self.processing_queue.task_done()
                    
                except queue.Empty:
                    pass
                    
                current_time = time.time()
                # Process batch if full or timeout occurred
                if (len(batch) >= BATCH_SIZE or 
                    (batch and current_time - last_process_time > BATCH_TIMEOUT_MS / 1000)):
                    self._process_batch(batch)
                    batch = []
                    last_process_time = current_time
                    
                # Check for shutdown signal
                if self.shutdown_event.is_set() and self.processing_queue.empty():
                    break
                    
            except Exception as e:
                logger.error(f"Worker thread {thread_name} error: {e}")
                CDC_ERRORS.inc()
                
        # Process remaining items in batch
        if batch:
            self._process_batch(batch)
            
        logger.info(f"Worker thread {thread_name} shutting down")
        
    def _process_message(self, value):
        """Process a single message from Kafka."""
        start_time = time.time()
        
        try:
            # Extract record data
            after = value.get("payload", {}).get("after")
            before = value.get("payload", {}).get("before")
            
            if not before and not after:
                CDC_SKIPPED_EVENTS.inc()
                return None
            
            # Determine operation type
            if not before and after:
                event_type = "INSERT"
                after_transformed = self._transform_record(after)
                operation = ("insert", after_transformed)
            elif before and after:
                event_type = "UPDATE"
                after_transformed = self._transform_record(after)
                operation = ("update", before["id"], after_transformed)
            else:  # before and not after
                event_type = "DELETE"
                operation = ("delete", before["id"])
            
            # Calculate metrics
            processing_latency = time.time() - start_time
            CDC_EVENT_LATENCY.observe(processing_latency)
            
            # Calculate lag
            event_time = value.get('timestamp', int(time.time() * 1000)) / 1000.0
            lag = time.time() - event_time
            CDC_CURRENT_LAG.set(lag)
            
            return operation, event_type
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            CDC_ERRORS.inc()
            return None
            
    def _transform_record(self, record):
        """Transform record by parsing JSON fields and converting datetime fields."""
        # Deep copy to avoid modifying original
        transformed = record.copy()
        
        # Transform JSON fields
        for field in JSON_FIELDS:
            if field in transformed and transformed[field]:
                try:
                    transformed[field] = json.loads(transformed[field])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse JSON field: {field}")
                    
        # Transform datetime fields
        for field in DATETIME_FIELDS:
            if field in transformed and transformed[field]:
                try:
                    timestamp_second = int(transformed[field] / microseconds)
                    transformed[field] = datetime.datetime.fromtimestamp(timestamp_second)
                except (ValueError, TypeError):
                    logger.warning(f"Failed to parse datetime field: {field}")
                    
        return transformed
    
    def _process_batch(self, batch):
        """Process a batch of operations in a worker thread."""
        if not batch:
            return
            
        batch_start = time.time()
        inserts = []
        updates = []
        deletes = []
        event_counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
        
        # Group operations by type
        for operation, event_type in batch:
            if operation[0] == "insert":
                inserts.append(operation[1])
                event_counts["INSERT"] += 1
            elif operation[0] == "update":
                updates.append(({"id": operation[1]}, operation[2]))
                event_counts["UPDATE"] += 1
            elif operation[0] == "delete":
                deletes.append({"id": operation[1]})
                event_counts["DELETE"] += 1
        
        # Execute operations in bulk
        try:
            if inserts:
                self.collection.insert_many(inserts, ordered=False)
                
            if updates:
                # Use bulk write for updates
                bulk_updates = []
                for filter_doc, update_doc in updates:
                    bulk_updates.append(pymongo.ReplaceOne(filter_doc, update_doc, upsert=True))
                if bulk_updates:
                    self.collection.bulk_write(bulk_updates, ordered=False)
                
            if deletes:
                self.collection.delete_many({"id": {"$in": [doc["id"] for doc in deletes]}})
                
            # Update metrics
            for event_type, count in event_counts.items():
                if count > 0:
                    CDC_EVENT_TYPES.labels(event_type=event_type, process_id=str(self.process_id)).inc(count)
                    CDC_EVENTS_PROCESSED.inc(count)
            
            # Send operation results to result queue
            self.result_queue.put(event_counts)
            
            batch_time = time.time() - batch_start
            CDC_BATCH_PROCESSING_TIME.observe(batch_time)
            CDC_BATCH_SIZE.observe(len(batch))
            
            logger.info(f"Process {self.process_id}: Processed batch of {len(batch)} records in {batch_time:.3f}s: {event_counts}")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            CDC_ERRORS.inc(len(batch))

def worker_process_main(process_id, message_queue, result_queue, shutdown_event):
    """Main function for worker processes."""
    try:
        logger.info(f"Worker process {process_id} started")
        processor = MessageProcessor(process_id, message_queue, result_queue, shutdown_event)
        processor.start()
        
        # Keep process alive until shutdown event
        while not shutdown_event.is_set():
            time.sleep(0.5)
            
        processor.shutdown()
        logger.info(f"Worker process {process_id} exiting cleanly")
        
    except Exception as e:
        logger.error(f"Worker process {process_id} error: {e}")

def consume_messages():
    """Main function that consumes messages from Kafka and distributes to processes."""
    # Create Kafka consumer
    config = {
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'group.id': os.getenv("KAFKA_GROUP_ID"),
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'max.poll.interval.ms': 300000,  # 5 minutes
        'session.timeout.ms': 30000,
        'fetch.max.bytes': 52428800,  # 50MB
        'max.partition.fetch.bytes': 1048576  # 1MB
    }
    
    if IS_AUTHENTICATE_KAFKA:
        config.update({
            'sasl.username': os.getenv("KAFKA_SASL_USERNAME"),
            'sasl.password': os.getenv("KAFKA_SASL_PASSWORD"),
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanisms': 'PLAIN'
        })
        
    consumer = Consumer(config)
    consumer.subscribe([os.getenv("KAFKA_TOPIC")])
    logger.info(f"Starting consume {os.getenv('KAFKA_TOPIC')} with {NUM_WORKER_PROCESSES} processes x {NUM_THREADS_PER_PROCESS} threads")
    
    # Create shared queues and event for inter-process communication
    message_queue = JoinableQueue(maxsize=MAX_QUEUE_SIZE)
    result_queue = Queue()
    shutdown_event = Event()
    
    # Start worker processes
    processes = []
    for i in range(NUM_WORKER_PROCESSES):
        p = Process(
            target=worker_process_main,
            args=(i, message_queue, result_queue, shutdown_event)
        )
        p.daemon = True
        p.start()
        processes.append(p)
    
    # Result collector thread
    def collect_results():
        """Thread to collect and aggregate results from worker processes."""
        total_processed = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
        
        while not shutdown_event.is_set():
            try:
                result = result_queue.get(timeout=0.5)
                # Update totals
                for event_type, count in result.items():
                    total_processed[event_type] += count
                    
                if sum(total_processed.values()) % 1000 == 0:
                    logger.info(f"Total processed: {sum(total_processed.values())} operations - {total_processed}")
                    
            except (queue.Empty, EOFError):
                continue
            except Exception as e:
                logger.error(f"Result collector error: {e}")
    
    # Start result collector
    collector = threading.Thread(target=collect_results)
    collector.daemon = True
    collector.start()
    
    try:
        last_commit_time = time.time()
        commit_interval = COMMIT_INTERVAL_MS / 1000.0
        
        while True:
            # Update queue size metric
            CDC_QUEUE_SIZE.labels(queue_name="main").set(message_queue.qsize())
            
            # Poll for messages with timeout
            msg = consumer.poll(0.1)
            
            if msg is None:
                # No message available
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    logger.error(f'Kafka error: {msg.error()}')
                    CDC_ERRORS.inc()
            else:
                try:
                    # Parse message and add to queue
                    value = json.loads(msg.value())
                    message_queue.put(value, timeout=5)
                except queue.Full:
                    logger.warning("Message queue is full, waiting...")
                    time.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error parsing message: {e}")
                    CDC_ERRORS.inc()
            
            # Commit offsets periodically
            current_time = time.time()
            if current_time - last_commit_time >= commit_interval:
                consumer.commit(asynchronous=False)
                last_commit_time = current_time
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Signal shutdown to worker processes
        shutdown_event.set()
        
        # Wait for message queue to be processed
        try:
            message_queue.join()
        except:
            pass
            
        # Final commit before closing
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Error committing offsets: {e}")
            
        consumer.close()
        
        # Terminate worker processes if they haven't exited
        for p in processes:
            if p.is_alive():
                p.join(timeout=5.0)
                if p.is_alive():
                    logger.warning(f"Terminating worker process {p.pid}")
                    p.terminate()
        
        logger.info("Shutdown complete")

if __name__ == "__main__":
    # Set up metrics server with multiprocess support
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    start_http_server(8000, registry=registry)
    logger.info("Prometheus metrics server started on port 8000")

    # Start consuming messages
    consume_messages()