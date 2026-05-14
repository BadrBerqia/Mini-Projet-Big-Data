import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send Amazon reviews to Kafka as a stream.")
    parser.add_argument("--csv", default="data/Reviews.csv", help="Path to the Amazon reviews CSV file.")
    parser.add_argument("--topic", default="reviews", help="Kafka topic name.")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka bootstrap server.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum number of rows to stream.")
    parser.add_argument("--sleep", type=float, default=0.05, help="Delay between messages in seconds.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    columns = ["UserId", "ProductId", "Score", "Time"]
    reviews = pd.read_csv(args.csv, usecols=columns).dropna().head(args.limit)

    for _, row in reviews.iterrows():
        event = {
            "user_id": str(row["UserId"]),
            "product_id": str(row["ProductId"]),
            "score": int(row["Score"]),
            "time": int(row["Time"]),
        }
        producer.send(args.topic, event)
        print(event, flush=True)
        time.sleep(args.sleep)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()

