# Presentation Day Guide - Mini-Projet Big Data

This guide is for running the project from a fresh PC start until Docker shutdown.

## 1. Start the PC

1. Turn on the PC.
2. Open Docker Desktop.
3. Wait until Docker shows that it is running.
4. Open PowerShell.

## 2. Go to the Project Folder

```powershell
cd "C:\Users\berqi\Documents\Mini-Projet-Big-Data"
```

Check that you are in the correct folder:

```powershell
pwd
```

Expected folder:

```text
C:\Users\berqi\Documents\Mini-Projet-Big-Data
```

## 3. Check the Dataset

The real Kaggle CSV must exist here:

```text
data\Reviews.csv
```

Check it:

```powershell
Test-Path data\Reviews.csv
```

Expected result:

```text
True
```

If it returns `False`, copy `Reviews.csv` into the `data` folder before continuing.

## 4. Start All Services

Use this command:

```powershell
docker compose up -d --build
```

Wait until the command finishes.

Check running services:

```powershell
docker compose ps
```

Expected containers:

```text
mnp_zookeeper
mnp_kafka
mnp_spark
mnp_spark_worker
mnp_airflow
mnp_api
```

## 5. Wait for Airflow

Airflow can take a little time to initialize.

Check logs:

```powershell
docker logs --tail 60 mnp_airflow
```

Look for messages showing that the scheduler and webserver started.

Open Airflow:

```text
http://localhost:8080
```

Login:

```text
Username: admin
Password: admin
```

## 6. Check the API

Open in the browser:

```text
http://localhost:8000/health
```

Expected result:

```json
{"status":"ok"}
```

Or use PowerShell:

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health"
```

## 7. Run the Airflow DAG

In Airflow:

1. Open the DAG named `recommendation_pipeline`.
2. Unpause it if it is paused.
3. Click the trigger button.
4. Wait until all tasks become green.

Task order:

```text
check_dataset
create_kafka_topic
stream_reviews_to_kafka
train_als_model
run_streaming_trigger
```

Alternative terminal test:

```powershell
docker exec mnp_airflow airflow dags test recommendation_pipeline 2026-05-15
```

## 8. Verify Recommendations

Open:

```text
http://localhost:8000/recommendations/user/A2R6RA8FRBS608
```

Expected result: a JSON response with a user id and recommended product ids.

Another useful user id:

```text
A3NHUQ33CFH3VM
```

Open:

```text
http://localhost:8000/recommendations/user/A3NHUQ33CFH3VM
```

## 9. Verify Recent Streaming Events

Open:

```text
http://localhost:8000/events/recent?limit=5
```

Expected result: recent review events with:

```text
user_id
product_id
score
time
```

## 10. Open the Dashboard

Open the dashboard file:

```text
dashboard\index.html
```

Use it to:

1. Search recommendations by user id.
2. Refresh recent streaming events.

Recommended demo user:

```text
A2R6RA8FRBS608
```

## 11. Useful URLs During the Demo

```text
API health:
http://localhost:8000/health

Recommendations:
http://localhost:8000/recommendations/user/A2R6RA8FRBS608

Recent events:
http://localhost:8000/events/recent?limit=5

Airflow:
http://localhost:8080

Spark Master:
http://localhost:8081
```

## 12. Useful Debug Commands

Check all containers:

```powershell
docker compose ps
```

Check API logs:

```powershell
docker logs --tail 80 mnp_api
```

Check Airflow logs:

```powershell
docker logs --tail 80 mnp_airflow
```

Check Kafka logs:

```powershell
docker logs --tail 80 mnp_kafka
```

List Kafka topics:

```powershell
docker exec mnp_kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Read a few Kafka messages:

```powershell
docker exec mnp_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic reviews --from-beginning --max-messages 5
```

## 13. If Something Does Not Work

### Docker is not running

Open Docker Desktop and wait until it is ready.

### API health does not answer

Check containers:

```powershell
docker compose ps
```

Restart API:

```powershell
docker compose restart api
```

### Airflow page does not open

Wait one minute, then check:

```powershell
docker logs --tail 80 mnp_airflow
```

### DAG fails because dataset is missing

Check:

```powershell
Test-Path data\Reviews.csv
```

If missing, place `Reviews.csv` in:

```text
data\Reviews.csv
```

### Reset everything

Use this only if you want a clean restart:

```powershell
docker compose down -v
docker compose up -d --build
```

## 14. Shut Down Docker After the Demo

Stop the project:

```powershell
docker compose down
```

Check that containers stopped:

```powershell
docker compose ps
```

If you want to stop Docker Desktop too, close it after the containers are down.

## 15. Short Demo Script

1. Show GitHub repository and README.
2. Show Docker services running with `docker compose ps`.
3. Open Airflow at `http://localhost:8080`.
4. Explain the DAG tasks.
5. Trigger the DAG or show a successful run.
6. Open API health endpoint.
7. Open recommendations endpoint.
8. Open recent events endpoint.
9. Open dashboard and search a user.
10. Mention metrics:

```text
Rows: 20000
Distinct users: 17677
Distinct products: 2657
Validation RMSE: about 1.8974
Test RMSE: about 1.9013
```

## 16. Final Shutdown

```powershell
cd "C:\Users\berqi\Documents\Mini-Projet-Big-Data"
docker compose down
```
