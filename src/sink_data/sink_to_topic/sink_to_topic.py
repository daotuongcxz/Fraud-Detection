import json
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
    KafkaSource)

JARS_PATH = f"{os.getcwd()}/data_ingestion/kafka_connect/jars/"


def extract_after_payload(record):
    record = json.loads(record)
    after = record.get("payload", {}).get("after")
    if after is None:
        return None
    return json.dumps(after)

def filter_small_features(record):
    """
    Skip records containing a feature that is smaller than 0.5.
    """
    # Convert Row to dict
    record = json.loads(record)

    # for key in record["payload"]:
    #     if key.startswith("feature"):
    #         if record["payload"][key] < 0.5:
    #             return False

    print("Found record: ", record)
    return True


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # The other commented lines are for Avro format
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
    )

    # Define the source to take data from
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29092")
        .set_topics("transactions.public.transactions")
        .set_group_id("device-consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Define the sink to save the processed data to
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("broker:29092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("transactions_2")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # No sink, just print out to the terminal
    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
    #     filter_small_features
    # ).map(merge_features).print()

    # Add a sink to be more industrial, remember to cast to STRING in map
    # it will not work if you don't do it
    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
        filter_small_features
    ).map(extract_after_payload, output_type=Types.STRING()).sink_to(sink=sink)

    # Execute the job
    env.execute("flink_datastream_demo")
    print("Your job has been started successfully!")


if __name__ == "__main__":
    main()
