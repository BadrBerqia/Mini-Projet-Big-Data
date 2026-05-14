# Architecture du mini-projet

Le projet met en place un pipeline Big Data pour recommander des produits a partir des interactions utilisateurs.

## Composants

- Kafka : ingestion des avis sous forme de flux.
- Spark MLlib : entrainement du modele ALS.
- Spark Structured Streaming : consommation du flux Kafka.
- Airflow : orchestration du pipeline.
- FastAPI : exposition des recommandations.
- Dashboard web : consultation des recommandations par utilisateur.

## Flux

1. Le producer lit `data/Reviews.csv`.
2. Les evenements sont envoyes vers le topic Kafka `reviews`.
3. Spark entraine un modele ALS sur les notes historiques.
4. Spark genere des recommandations Top-N.
5. L'API lit les recommandations sauvegardees.
6. Le dashboard interroge l'API.

