import json
import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan


DATA_PATH = "data/Reviews.csv"
MODEL_PATH = "data/models/als_model"
RECOMMENDATIONS_PATH = "data/recommendations.json"
METRICS_PATH = "data/metrics.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train an ALS recommendation model.")
    parser.add_argument("--data", default=DATA_PATH, help="Path to Reviews.csv.")
    parser.add_argument("--model-path", default=MODEL_PATH, help="Output path for the ALS model.")
    parser.add_argument("--recommendations-path", default=RECOMMENDATIONS_PATH)
    parser.add_argument("--metrics-path", default=METRICS_PATH)
    parser.add_argument("--limit", type=int, default=5000, help="Optional row limit for fast tests.")
    parser.add_argument("--rank", type=int, default=10)
    parser.add_argument("--max-iter", type=int, default=10)
    parser.add_argument("--reg-param", type=float, default=0.1)
    parser.add_argument("--top-n", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("MNP ALS Training")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    reviews = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(args.data)
        .select("UserId", "ProductId", "Score", "Time")
        .dropna()
        .withColumn("Score", col("Score").cast("float"))
        .filter(col("Score").isNotNull())
        .filter(~isnan(col("Score")))
        .filter((col("Score") >= 1) & (col("Score") <= 5))
    )

    if args.limit and args.limit > 0:
        reviews = reviews.limit(args.limit)

    total_rows = reviews.count()
    distinct_users = reviews.select("UserId").distinct().count()
    distinct_products = reviews.select("ProductId").distinct().count()

    user_indexer = StringIndexer(inputCol="UserId", outputCol="user_index", handleInvalid="skip")
    product_indexer = StringIndexer(inputCol="ProductId", outputCol="product_index", handleInvalid="skip")

    user_indexer_model = user_indexer.fit(reviews)
    indexed = user_indexer_model.transform(reviews)

    product_indexer_model = product_indexer.fit(indexed)
    indexed = product_indexer_model.transform(indexed)

    ratings = indexed.select(
        col("user_index").cast("int"),
        col("product_index").cast("int"),
        col("Score").alias("rating"),
    )

    train, validation, test = ratings.randomSplit([0.8, 0.1, 0.1], seed=42)

    als = ALS(
        userCol="user_index",
        itemCol="product_index",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
        rank=args.rank,
        maxIter=args.max_iter,
        regParam=args.reg_param,
    )

    model = als.fit(train)
    predictions = model.transform(validation).dropna(subset=["prediction"])

    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction",
    )
    validation_rmse = evaluator.evaluate(predictions) if predictions.count() else None
    print(
        f"Validation RMSE: {validation_rmse:.4f}"
        if validation_rmse is not None
        else "Validation RMSE: not available"
    )

    model_path = Path(args.model_path)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    if model_path.exists():
        try:
            if model_path.is_dir():
                shutil.rmtree(model_path)
            else:
                model_path.unlink()
        except PermissionError:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            model_path = model_path.parent / f"{model_path.name}_{timestamp}"
            print(f"Could not remove existing model path; saving model to {model_path}")
    model_saved = True
    model_save_error = None
    try:
        model.write().overwrite().save(str(model_path))
    except Exception as exc:
        model_saved = False
        model_save_error = str(exc).splitlines()[0]
        print(f"Could not save Spark model artifact: {model_save_error}")

    user_labels = user_indexer_model.labels
    product_labels = product_indexer_model.labels
    recommendations = model.recommendForAllUsers(args.top_n).limit(50).collect()
    output = {
        user_labels[int(row["user_index"])]: [
            product_labels[int(item["product_index"])] for item in row["recommendations"]
        ]
        for row in recommendations
    }

    Path(args.recommendations_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.recommendations_path).write_text(
        json.dumps(output, indent=2),
        encoding="utf-8",
    )

    test_predictions = model.transform(test).dropna(subset=["prediction"])
    test_rmse = evaluator.evaluate(test_predictions) if test_predictions.count() else None
    print(
        f"Test RMSE: {test_rmse:.4f}"
        if test_rmse is not None
        else "Test RMSE: not available"
    )

    metrics = {
        "rows": total_rows,
        "distinct_users": distinct_users,
        "distinct_products": distinct_products,
        "rank": args.rank,
        "max_iter": args.max_iter,
        "reg_param": args.reg_param,
        "top_n": args.top_n,
        "model_path": str(model_path),
        "model_saved": model_saved,
        "model_save_error": model_save_error,
        "validation_rmse": validation_rmse,
        "test_rmse": test_rmse,
    }
    Path(args.metrics_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.metrics_path).write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    spark.stop()


if __name__ == "__main__":
    main()
