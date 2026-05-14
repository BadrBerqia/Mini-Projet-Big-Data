import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "reviews"
OUTPUT_PATH = "data/streaming_events"
CHECKPOINT_PATH = "data/checkpoints/reviews_stream"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume review events from Kafka with Spark.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--output-path", default=OUTPUT_PATH)
    parser.add_argument("--checkpoint-path", default=CHECKPOINT_PATH)
    parser.add_argument("--trigger-once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("MNP Kafka Streaming")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("time", IntegerType(), True),
        ]
    )

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    events = raw_events.select(
        from_json(col("value").cast("string"), schema).alias("event")
    ).select("event.*")

    writer = (
        events.writeStream.format("json")
        .outputMode("append")
        .option("path", args.output_path)
        .option("checkpointLocation", args.checkpoint_path)
    )

    if args.trigger_once:
        writer = writer.trigger(once=True)

    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
