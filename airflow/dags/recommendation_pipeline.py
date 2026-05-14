from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="recommendation_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mnp", "kafka", "spark", "recommendation"],
) as dag:
    check_dataset = BashOperator(
        task_id="check_dataset",
        bash_command="test -f /app/data/Reviews.csv",
    )

    create_kafka_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command=(
            "python /app/scripts/create_kafka_topic.py "
            "--bootstrap-server kafka:29092 "
            "--topic reviews"
        ),
    )

    stream_reviews_to_kafka = BashOperator(
        task_id="stream_reviews_to_kafka",
        bash_command=(
            "python /app/producer/producer.py "
            "--csv /app/data/Reviews.csv "
            "--bootstrap-server kafka:29092 "
            "--topic reviews "
            "--limit 100 "
            "--sleep 0.01"
        ),
    )

    train_model = BashOperator(
        task_id="train_als_model",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--conf spark.ui.showConsoleProgress=false "
            "/app/spark/train_als.py "
            "--data /app/data/Reviews.csv "
            "--model-path /app/data/models/als_model "
            "--recommendations-path /app/data/recommendations.json "
            "--metrics-path /app/data/metrics.json "
            "--limit 20000 "
            "--rank 10 "
            "--max-iter 8 "
            "--reg-param 0.1 "
            "--top-n 5"
        ),
    )

    run_streaming_trigger = BashOperator(
        task_id="run_streaming_trigger",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 "
            "--conf spark.ui.showConsoleProgress=false "
            "/app/spark/streaming_recommendations.py "
            "--bootstrap-servers kafka:29092 "
            "--topic reviews "
            "--output-path /app/data/streaming_events "
            "--checkpoint-path /app/data/checkpoints/reviews_stream "
            "--trigger-once"
        ),
    )

    check_dataset >> create_kafka_topic >> stream_reviews_to_kafka >> train_model >> run_streaming_trigger
