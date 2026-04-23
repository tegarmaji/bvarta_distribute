import argparse
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window


# ============================================================
# Config & Spark
# ============================================================

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_spark(app_name: str = "de-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


## Explicit Schemas
## All event columns read as StringType to avoid error 
RAW_EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(), True),
    StructField("user_id",    StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts",   StringType(), True),
    StructField("value",      StringType(), True),  # may arrive as "30" or null
])

USERS_SCHEMA = StructType([
    StructField("user_id",     StringType(), True),
    StructField("country",     StringType(), True),
    StructField("signup_date", StringType(), True),
])



## 1. Ingestion (read JSONL files)
def ingest_raw(spark: SparkSession, input_glob: str) -> DataFrame:
    return spark.read.schema(RAW_EVENT_SCHEMA).json(input_glob)


## 2. Bronze Layer (sanitize & deduplicate)
def bronze(spark: SparkSession, raw: DataFrame, output_path: str) -> DataFrame:
    #  Normalise strings using trim & upper 
    df = (
        raw
        .withColumn("event_id",   F.trim(F.col("event_id")))
        .withColumn("user_id",    F.trim(F.col("user_id")))
        .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
    )

    # Parse event_ts into timestamp datatype
    # other format will be treated invalid and default as null
    _TS_FORMATS = [
        "yyyy-MM-dd'T'HH:mm:ss'Z'",       # 2025-01-01T10:00:00Z      (expected as per data sample)
    ]
    df = df.withColumn(
        "event_ts_parsed",
        F.coalesce(*[
            F.try_to_timestamp(F.col("event_ts"), F.lit(fmt))
            for fmt in _TS_FORMATS
        ])
    )

    # Cast to float/double type
    df = df.withColumn("value_parsed", F.col("value").cast(DoubleType()))

    #  Tag rejection
    df = df.withColumn(
        "rejection_reason",
        F.when(
            F.col("event_id").isNull() | (F.col("event_id") == ""),
            F.lit("missing_event_id")
        ).when(
            F.col("user_id").isNull() | (F.col("user_id") == ""),
            F.lit("missing_user_id")
        ).when(
            F.col("event_type").isNull() | (F.col("event_type") == ""),
            F.lit("missing_event_type")
        ).when(
            F.col("event_ts_parsed").isNull(),
            F.lit("invalid_event_ts")
        ).otherwise(F.lit(None).cast(StringType()))
    )

    #  Separate clean vs quarantine 
    quarantine = df.filter(F.col("rejection_reason").isNotNull())
    clean      = df.filter(F.col("rejection_reason").isNull())

    #  Write parquet for quarantine
    (
        quarantine
        .select("event_id", "user_id", "event_type", "event_ts", "value", "rejection_reason")
        .write.mode("overwrite")
        .parquet(f"{output_path}/quarantine")
    )

    #  Deduplicate: per event_id keep the record with the latest ts 
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("event_ts_parsed").desc())
    clean = (
        clean
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    #  Fill missing value with 0.0 
    clean = clean.withColumn(
        "value_parsed",
        F.coalesce(F.col("value_parsed"), F.lit(0.0))
    )

    #  Project final Bronze schema 
    clean = clean.select(
        F.col("event_id"),
        F.col("user_id"),
        F.col("event_type"),
        F.col("event_ts_parsed").alias("event_ts"),
        F.col("value_parsed").alias("value"),
        F.to_date(F.col("event_ts_parsed")).alias("event_date"),
    )

    #  Write parquet for clean
    (
        clean
        .write
        .partitionBy("event_date")
        .mode("overwrite")
        .parquet(f"{output_path}/clean")
    )

    return clean


## 3. Silver Layer : Enrich & Derive Fields
def silver(spark: SparkSession, clean: DataFrame, users_path: str, output_path: str) -> DataFrame:
    #  Load & clean users 
    users = (
        spark.read
        .schema(USERS_SCHEMA)
        .option("header", True)
        .csv(users_path)
        .withColumn(
            "signup_date_parsed",
            F.try_to_timestamp(F.col("signup_date"), F.lit("yyyy-MM-dd")).cast("date")
        )
        .withColumn(
            "country",
            F.when(
                F.col("country").isNull() | (F.trim(F.col("country")) == ""),
                F.lit("UNKNOWN")
            ).otherwise(F.upper(F.trim(F.col("country"))))
        )
        .select(
            "user_id",
            "country",
            F.col("signup_date_parsed").alias("signup_date"),
        )
    )

    #  Enrich 
    enriched = clean.join(users, on="user_id", how="left")

    #  Derived fields 
    enriched = (
        enriched
        .withColumn("is_purchase", (F.col("event_type") == F.lit("PURCHASE")))
        .withColumn(
            "days_since_signup",
            F.when(
                F.col("signup_date").isNotNull(),
                F.datediff(F.col("event_date"), F.col("signup_date"))
            ).otherwise(F.lit(None).cast("int"))
        )
        # Ensure every row has a country even for unmatched users
        .withColumn(
            "country",
            F.coalesce(F.col("country"), F.lit("UNKNOWN"))
        )
    )

    #  Write parquet
    (
        enriched
        .write
        .partitionBy("event_date")
        .mode("overwrite")
        .parquet(output_path)
    )

    return enriched


## 4. Gold Layer — Daily Country-Level Aggregations
def gold(enriched: DataFrame, output_path: str) -> None:
    agg = (
        enriched
        .groupBy("event_date", "country")
        .agg(
            F.count("event_id").alias("total_events"),
            F.sum("value").alias("total_value"),
            F.sum(F.col("is_purchase").cast("int")).alias("total_purchases"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        # Explicit column order so the schema is predictable for any reader.
        .select(
            "event_date",
            "country",
            "total_events",
            "total_value",
            "total_purchases",
            "unique_users",
        )
    )

    # Write parquet
    (
        agg
        .write
        .partitionBy("event_date")
        .mode("overwrite")
        .parquet(output_path)
    )



## Main
def main(config_path: str):
    config = load_config(config_path)
    spark  = get_spark()

    paths = config["paths"]

    # 1 — Ingestion
    raw = ingest_raw(spark, paths["input"])

    # 2 — Bronze (clean + quarantine)
    clean = bronze(spark, raw, paths["bronze"])

    # 3 — Silver (enriched)
    enriched = silver(spark, clean, paths["users"], paths["silver"])

    # 4 — Gold (aggregated)
    gold(enriched, paths["gold"])

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medallion batch pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    args = parser.parse_args()
    main(args.config)