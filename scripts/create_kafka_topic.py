import argparse

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a Kafka topic if it does not exist.")
    parser.add_argument("--bootstrap-server", default="kafka:29092")
    parser.add_argument("--topic", default="reviews")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_server, client_id="mnp-airflow")
    topic = NewTopic(
        name=args.topic,
        num_partitions=args.partitions,
        replication_factor=args.replication_factor,
    )

    try:
        admin.create_topics([topic])
        print(f"Created topic {args.topic}")
    except TopicAlreadyExistsError:
        print(f"Topic {args.topic} already exists")
    finally:
        admin.close()


if __name__ == "__main__":
    main()

