# Mini-Projet Big Data - Real-Time Product Recommendation

This project implements a Big Data pipeline for product recommendation using the Amazon Fine Food Reviews dataset. It combines Kafka, Spark MLlib, Spark Structured Streaming, Airflow, FastAPI, and a small web dashboard.

## Objective

The goal is to simulate a real-time recommendation system:

1. Historical product reviews are used to train an ALS recommendation model with Spark MLlib.
2. A Kafka producer sends review events as a stream.
3. Spark Structured Streaming consumes Kafka events and stores recent activity.
4. Airflow orchestrates the full pipeline.
5. FastAPI exposes recommendations and recent streaming events.
6. A dashboard allows manual testing from the browser.

## Architecture

```text
Reviews.csv
   |
   | historical data
   v
Spark MLlib ALS  --->  data/recommendations.json  --->  FastAPI  --->  Dashboard

Reviews.csv
   |
   | simulated real-time events
   v
Kafka topic: reviews  --->  Spark Structured Streaming  --->  data/streaming_events

Airflow DAG orchestrates:
dataset check -> Kafka topic -> producer -> ALS training -> streaming trigger
```

## Technologies

- Docker Compose
- Apache Kafka
- Apache Spark 3.5.8
- Spark MLlib ALS
- Spark Structured Streaming
- Apache Airflow 2.9.1
- FastAPI
- HTML/CSS/JavaScript dashboard

## Project Structure

```text
.
|-- airflow/              Airflow image, startup script, and DAG
|-- api/                  FastAPI application
|-- dashboard/            Browser dashboard
|-- data/                 Local dataset, sample data, generated outputs
|-- docs/                 Architecture notes and local guides
|-- producer/             Kafka producer
|-- scripts/              Kafka topic helper scripts
|-- spark/                Spark ALS and streaming jobs
|-- docker-compose.yml    Service orchestration
`-- README.md
```

## Dataset

The project uses the Kaggle Amazon Fine Food Reviews dataset.

Download the archive from Kaggle, extract it, and place the CSV here:

```text
data/Reviews.csv
```

The full dataset is not committed to GitHub because it is large. The repository includes a small sample file:

```text
data/reviews_sample.csv
```

## Start the Project

From the project folder:

```powershell
cd "C:\Users\berqi\Documents\Mini-Projet-Big-Data"
docker compose up -d --build
```

Useful URLs:

- API health: <http://localhost:8000/health>
- Recent events: <http://localhost:8000/events/recent?limit=5>
- Example recommendations: <http://localhost:8000/recommendations/user/A2R6RA8FRBS608>
- Airflow: <http://localhost:8080>
- Spark Master: <http://localhost:8081>
- Dashboard: open `dashboard/index.html` in the browser

Airflow credentials:

```text
Username: admin
Password: admin
```

## Airflow Pipeline

The main DAG is:

```text
recommendation_pipeline
```

It runs the following tasks:

1. `check_dataset`
2. `create_kafka_topic`
3. `stream_reviews_to_kafka`
4. `train_als_model`
5. `run_streaming_trigger`

To test the full DAG from the terminal:

```powershell
docker exec mnp_airflow airflow dags test recommendation_pipeline 2026-05-15
```

To run it from the UI:

1. Open <http://localhost:8080>
2. Login with `admin` / `admin`
3. Open `recommendation_pipeline`
4. Unpause the DAG if needed
5. Click the trigger button
6. Wait until all tasks are green

## API Endpoints

Health check:

```text
GET /health
```

Recommendations for a user:

```text
GET /recommendations/user/{user_id}
```

Recent streamed events:

```text
GET /events/recent?limit=5
```

Example:

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/recommendations/user/A2R6RA8FRBS608"
Invoke-RestMethod -Uri "http://localhost:8000/events/recent?limit=5"
```

## Manual Spark Commands

Train the ALS model:

```powershell
docker exec mnp_spark /opt/spark/bin/spark-submit `
  --master local[*] `
  --conf spark.ui.showConsoleProgress=false `
  /app/spark/train_als.py `
  --data /app/data/Reviews.csv `
  --model-path /app/data/models/als_model `
  --recommendations-path /app/data/recommendations.json `
  --metrics-path /app/data/metrics.json `
  --limit 20000 `
  --rank 10 `
  --max-iter 8 `
  --reg-param 0.1 `
  --top-n 5
```

Run Spark streaming once:

```powershell
docker exec mnp_spark /opt/spark/bin/spark-submit `
  --master local[*] `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 `
  --conf spark.ui.showConsoleProgress=false `
  /app/spark/streaming_recommendations.py `
  --bootstrap-servers kafka:29092 `
  --topic reviews `
  --output-path /app/data/streaming_events `
  --checkpoint-path /app/data/checkpoints/reviews_stream `
  --trigger-once
```

## Outputs

Generated local outputs include:

```text
data/recommendations.json
data/metrics.json
data/models/
data/streaming_events/
data/checkpoints/
```

These outputs are ignored by Git because they are generated during execution.

Example metrics from the tested pipeline:

```text
Rows: 20000
Distinct users: 17677
Distinct products: 2657
Validation RMSE: 1.8974
Test RMSE: 1.9013
```

## Stop the Project

Stop the containers:

```powershell
docker compose down
```

Stop and remove volumes if a full reset is needed:

```powershell
docker compose down -v
```

## Notes

- `Reviews.csv` must exist before running the full pipeline.
- The project is configured for local demonstration, not production deployment.
- Airflow uses a simple SQLite database and `SequentialExecutor`.
- The ALS training limit can be increased, but the demo uses `20000` rows for speed.
