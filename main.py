import json
import logging
import os
import pymongo
import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Counter, Gauge, Summary
import time

logger = logging.getLogger(__name__)

load_dotenv()

IS_AUTHENTICATE_KAFKA = int(os.getenv("IS_AUTHENTICATE_KAFKA", "1"))
JSON_FIELDS = os.getenv("JSON_FIELDS", "attach_field").split(",")
DATETIME_FIELDS = os.getenv("DATETIME_FIELDS", "created_at,updated_at").split(",")

mongo_client = pymongo.MongoClient(os.getenv("MONGO_URI"))
db = mongo_client.get_database(os.getenv("MONGO_DB"))
collection = db.get_collection(os.getenv("MONGO_COLLECTION"))
microseconds = 1000000

# Prometheus metrics
CDC_EVENTS_PROCESSED = Counter('cdc_events_processed_total', 'Total number of CDC events processed')
CDC_EVENT_LATENCY = Summary('cdc_event_processing_latency_seconds', 'Time taken to process each CDC event')
CDC_ERRORS = Counter('cdc_errors_total', 'Total number of errors encountered while processing CDC events')
CDC_SKIPPED_EVENTS = Counter('cdc_skipped_events_total', 'Total number of CDC events skipped')
CDC_CURRENT_LAG = Gauge('cdc_current_lag_seconds', 'Estimated lag between the CDC event generation and processing')
CDC_EVENT_TYPES = Counter('cdc_event_types_total', 'Number of CDC events processed, by type', ['event_type'])

def consume_messages():
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'group.id': os.getenv("KAFKA_GROUP_ID"),
        'auto.offset.reset': 'earliest'
    }
    if IS_AUTHENTICATE_KAFKA:
        config.update({
            'sasl.username': os.getenv("KAFKA_SASL_USERNAME"),
            'sasl.password': os.getenv("KAFKA_SASL_PASSWORD"),
            # Fixed properties
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanisms': 'PLAIN'
        })
    consumer = Consumer(config)
    consumer.subscribe([os.getenv("KAFKA_TOPIC")])
    logger.info("Starting consume {}".format(os.getenv("KAFKA_TOPIC")))

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, consumer reached end of log
                    continue
                else:
                    logger.error('Get message fail with ERROR {}'.format(msg.error()))
                    CDC_ERRORS.inc()
                    continue
            else:
                start_time = time.time()
                value = json.loads(msg.value())
                # Assuming ExtractNewRecordState was not used and you have 'after':
                after = value.get("payload", {}).get("after")
                before = value.get("payload", {}).get("before")
                if not before and not after:
                    CDC_SKIPPED_EVENTS.inc()
                    continue
                try:
                    # Transform
                    for field in JSON_FIELDS:
                        if after.get(field):
                            after[field] = json.loads(after[field])
                    for field in DATETIME_FIELDS:
                        if after.get(field):
                            timestamp_second = int(after[field] / microseconds)
                            after[field] = datetime.datetime.fromtimestamp(timestamp_second)
                except json.JSONDecodeError:
                    # Handle invalid JSON
                    CDC_ERRORS.inc()
                    CDC_SKIPPED_EVENTS.inc()
                    continue
                else:
                    # Insert
                    event_type = "INSERT"
                    if not before and after:
                        collection.insert_one(after)
                        logger.debug("1 record inserted")
                    # Update
                    elif before and after:
                        collection.find_one_and_replace({"id": before["id"]}, after)
                        event_type = "UPDATE"
                        logger.debug("1 record updated")
                    # Delete - before and not after
                    else:
                        collection.delete_one({"id": before["id"]})
                        event_type = "DELETE"
                        logger.debug("1 record deleted")
                    CDC_EVENT_TYPES.labels(event_type=event_type).inc()
                    processing_latency = time.time() - start_time
                    CDC_EVENT_LATENCY.observe(processing_latency)
                    event_time = value.get('timestamp', int(time.time() * 1000)) / 1000.0
                    lag = time.time() - event_time
                    CDC_CURRENT_LAG.set(lag)
                    CDC_EVENTS_PROCESSED.inc()
                    logger.info("Processed 1 record")

    except KeyboardInterrupt:
        pass
    finally:
        # Clean up
        consumer.close()


if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")

    # Start consuming messages
    consume_messages()
