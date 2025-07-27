import json
import os
from datetime import datetime
from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink

JARS_PATH = f"{os.getcwd()}/data_ingestion/kafka_connect/jars/"

# Define the output type with positional fields
ROW_TYPE = Types.ROW([
    Types.STRING(),  # transaction_id (position 0)
    Types.INT(),     # user_id (position 1)
    Types.DOUBLE(),  # amount (position 2)
    Types.STRING(),  # currency (position 3)
    Types.STRING(),  # merchant (position 4)
    Types.STRING(),  # timestamp (position 5)
    Types.STRING(),  # location (position 6)
    Types.INT()      # is_fraud (position 7)
])

def extract_after_payload(record):
    """
    Extract the 'after' payload from the Debezium CDC record and convert to Row
    """
    try:
        record = json.loads(record)
        after = record.get("payload", {}).get("after")
        if after is None:
            return None
        
        # Convert timestamp from microseconds to datetime string
        if 'timestamp' in after and isinstance(after['timestamp'], int):
            after['timestamp'] = datetime.fromtimestamp(after['timestamp'] / 1e6).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # Create Row with positional arguments (don't use named fields)
        return Row(
            str(after['transaction_id']),    # position 0
            int(after['user_id']),           # position 1
            float(after['amount']),          # position 2
            str(after['currency']),          # position 3
            str(after['merchant']),          # position 4
            str(after['timestamp']),         # position 5
            str(after['location']),          # position 6
            int(after['is_fraud'])           # position 7
        )
    except Exception as e:
        print(f"Error processing record: {e}")
        return None

def create_jdbc_sink():
    """
    Create a JDBC sink for PostgreSQL
    """
    connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url("jdbc:postgresql://timescaledb:5432/k6") \
        .with_driver_name("org.postgresql.Driver") \
        .with_user_name("k6") \
        .with_password("k6") \
        .build()
    
    execution_options = JdbcExecutionOptions.builder() \
        .with_batch_size(1000) \
        .with_batch_interval_ms(200) \
        .with_max_retries(5) \
        .build()
    
    # SQL statement for upsert
    sql = """
    INSERT INTO transactions (
        transaction_id, user_id, amount, currency, 
        merchant, timestamp, location, is_fraud
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (transaction_id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        merchant = EXCLUDED.merchant,
        timestamp = EXCLUDED.timestamp,
        location = EXCLUDED.location,
        is_fraud = EXCLUDED.is_fraud
    """
    
    return JdbcSink.sink(
        sql=sql,
        type_info=ROW_TYPE,
        jdbc_execution_options=execution_options,
        jdbc_connection_options=connection_options
    )

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add required JARs
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
        f"file://{JARS_PATH}/flink-connector-jdbc-3.1.0-1.17.jar",
        f"file://{JARS_PATH}/postgresql-42.4.3.jar"
    )

    # Define the Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29092")
        .set_topics("analytics.public.transactions")
        .set_group_id("postgres-sink-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Process the stream with proper type information
    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
        .map(extract_after_payload, output_type=ROW_TYPE) \
        .filter(lambda x: x is not None)

    # Add the JDBC sink
    ds.add_sink(create_jdbc_sink())

    # Execute the job
    env.execute("kafka_to_postgresql")
    print("Job started successfully!")

if __name__ == "__main__":
    main()