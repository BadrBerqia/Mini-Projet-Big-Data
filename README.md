# Mini-projet MNP : Systeme de recommandation temps reel

Ce projet realise un pipeline Big Data pour generer des recommandations de produits a partir du dataset Amazon Fine Food Reviews.

## Structure

- `data/` : dataset, modeles et recommandations generees.
- `producer/` : producer Kafka qui simule un flux temps reel.
- `spark/` : jobs Spark pour ALS et streaming Kafka.
- `airflow/dags/` : DAG Airflow d'orchestration.
- `api/` : API FastAPI pour exposer les recommandations.
- `dashboard/` : interface web simple.
- `docs/` : documentation du projet.

## Preparation

Place le fichier Kaggle `Reviews.csv` dans :

```text
data/Reviews.csv
```

## Demarrage des services

```bash
docker compose up -d zookeeper kafka spark spark-worker airflow api
```

Interfaces utiles :

- Airflow : http://localhost:8080
- Spark Master : http://localhost:8081
- API : http://localhost:8000/health

Identifiants Airflow :

- utilisateur : `admin`
- mot de passe : `admin`

## Creer le topic Kafka

```powershell
.\scripts\create_kafka_topic.ps1
```

## Lancer le producer Kafka

Depuis la machine locale :

```bash
pip install -r producer/requirements.txt
python producer/producer.py --csv data/Reviews.csv --limit 100
```

## Entrainement ALS

Dans le conteneur Spark :

```bash
docker exec -it mnp_spark spark-submit /app/spark/train_als.py
```

## Dashboard

Ouvre le fichier :

```text
dashboard/index.html
```

L'API doit etre lancee pour que la recherche fonctionne.

