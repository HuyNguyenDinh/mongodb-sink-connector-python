# MongoDB Sink Python

This project is a MongoDB sink written in Python. It reads data from a source and writes it to a MongoDB database.

## Prerequisites

- Python 3.10 or higher
- MongoDB

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/huynguyendinh/mongodb-sink-python.git
    cd mongodb-sink-python
    ```

2. Create a virtual environment and activate it:
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    ```

3. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

4. Copy the `.env.template` to `.env` and fill in the required environment variables:
    ```sh
    cp .env.template .env
    ```

## Configuration

The application uses environment variables for configuration. The `.env.template` file contains the following variables:

### MongoDB Connection
```
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=your_db_name
MONGO_COLLECTION_NAME=your_collection_name
TRANSFORM_FUNCTION=your_transform_function
```

- `MONGO_URI`: The URI of your MongoDB instance.
- `MONGO_DB_NAME`: The name of the database to use.
- `MONGO_COLLECTION`: The name of the collection to use.

### Kafka Connection
```
/**
 * IS_AUTHENTICATE_KAFKA: A flag indicating whether Kafka authentication is enabled (1 for enabled, 0 for disabled).
 * 
 * KAFKA.BOOTSTRAP.SERVERS: The Kafka bootstrap servers, specified as a comma-separated list of host:port pairs.
 * 
 * KAFKA.SASL.USERNAME: The username for SASL authentication with Kafka.
 * 
 * KAFKA.SASL.PASSWORD: The password for SASL authentication with Kafka.
 * 
 * KAFKA.GROUP.ID: The Kafka consumer group ID used for identifying the group of consumers.
 * 
 * KAFKA.TOPIC: The Kafka topic to which messages are published or from which messages are consumed.
 */
```
#### Example Kafka Connection

To configure the Kafka connection, you can set the following environment variables in your `.env` file:

```
IS_AUTHENTICATE_KAFKA=1
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SASL_USERNAME=your_kafka_username
KAFKA_SASL_PASSWORD=your_kafka_password
KAFKA_GROUP_ID=your_consumer_group_id
KAFKA_TOPIC=your_kafka_topic
```

- `IS_AUTHENTICATE_KAFKA`: Set to `1` to enable Kafka authentication, or `0` to disable it.
- `KAFKA_BOOTSTRAP_SERVERS`: The Kafka bootstrap servers, specified as a comma-separated list of host:port pairs.
- `KAFKA_SASL_USERNAME`: The username for SASL authentication with Kafka.
- `KAFKA_SASL_PASSWORD`: The password for SASL authentication with Kafka.
- `KAFKA_GROUP_ID`: The Kafka consumer group ID used for identifying the group of consumers.
- `KAFKA_TOPIC`: The Kafka topic to which messages are published or from which messages are consumed.

### Transform Configuation

The application uses two lists for field processing: `JSON_FIELDS` and `DATETIME_FIELDS`.

- `JSON_FIELDS`: A list of field names seperate by "," comma.
- `DATETIME_FIELDS`: A list of field names seperate by "," comma.

Example:
```python
JSON_FIELDS = "field1,field2,field3"
DATETIME_FIELDS = "field4,field5,field6"
```

## Usage

Run the main script to start the application:

```sh
python main.py
```

## main.py

The `main.py` script contains the main logic for reading data from the source, transforming it using the specified function, and writing it to MongoDB. Ensure that your environment variables are correctly set before running the script.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes.

## Contact

For any questions or inquiries, please contact [huyn27316@gmail.com].

