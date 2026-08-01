"""
Batch ETL job: historical real estate transactions.

PIPELINE STAGES (each explained where it happens below):
  1. EXTRACT : read the raw CSV as-is (all columns as strings, no guessing).
  2. LOAD (raw)   : write an unmodified copy into the Data Lake (MinIO),
                    preserving the source exactly as received.
  3. TRANSFORM    : deduplicate, fix formatting issues, normalize types.
  4. LOAD (curated): build the star schema (dimensions + fact) and write it
                     into the Data Warehouse (PostgreSQL).

WHY WRITE THE RAW COPY BEFORE CLEANING (step 2):
A Data Lake's job is to preserve the source of truth exactly as it arrived,
even with its flaws. If a cleaning rule turns out to be wrong later, or if
a new analysis needs a field we don't currently clean, we can always go
back to this raw copy and reprocess -- without needing to re-extract from
the original system. This is the core reason lakes and warehouses are kept
as two separate zones instead of only keeping the cleaned result.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RAW_CSV_PATH = "file:///opt/spark-data/historical_transactions.csv"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
RAW_ZONE_PATH = "s3a://raw-zone/historical_transactions/"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/real_estate_dwh"
POSTGRES_PROPERTIES = {
    "user": "dwh_user",
    "password": "dwh_password123",
    "driver": "org.postgresql.Driver",
}


def build_spark_session() -> SparkSession:
    """
    Create the SparkSession with the connectors it needs:
      - hadoop-aws: lets Spark talk to any S3-compatible store (MinIO here,
        real AWS S3 later, with zero code change -- only this config block
        would change).
      - postgresql JDBC driver: lets Spark write DataFrames straight into
        Postgres tables via SQL, without a separate ingestion tool.
    """
    spark = (
        SparkSession.builder.appName("RealEstateBatchETL")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    return spark


def extract(spark: SparkSession):
    """
    Stage 1: EXTRACT.

    Every column is read as StringType on purpose, not inferred. Letting
    Spark guess types on a file we know has formatting issues (price_mad
    is sometimes "1,250,000" as text) would make it silently misinterpret
    or null out values. We control every conversion explicitly in the
    transform stage instead.
    """
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("listing_id", StringType(), True),
        StructField("property_type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("surface_m2", StringType(), True),
        StructField("price_mad", StringType(), True),
        StructField("registration_status", StringType(), True),
        StructField("source_office", StringType(), True),
        StructField("transaction_date", StringType(), True),
    ])
    return spark.read.option("header", True).schema(schema).csv(RAW_CSV_PATH)


def load_raw_to_lake(raw_df):
    """
    Stage 2: LOAD (raw zone).

    Written in Parquet: a columnar, compressed format that is the de facto
    standard for data lakes (much cheaper to scan than CSV for later
    reprocessing). The CONTENT is still the untouched raw data -- only the
    file format changes, not the values.
    """
    raw_df.write.mode("overwrite").parquet(RAW_ZONE_PATH)
    print(f"Raw data written to {RAW_ZONE_PATH}")


def transform(raw_df):
    """
    Stage 3: TRANSFORM -- fixes the 4 documented data quality issues.
    """
    # Issue: full duplicate rows (same transaction_id re-exported twice).
    # We keep the FIRST occurrence of each transaction_id.
    df = raw_df.dropDuplicates(["transaction_id"])

    # Issue: price_mad sometimes formatted as text with thousand separators,
    # e.g. "1,250,000". Stripping commas before casting to double handles
    # both formats uniformly.
    df = df.withColumn(
        "price_mad_clean",
        F.regexp_replace(F.col("price_mad"), ",", "").cast("double"),
    )

    # Issue: missing surface_m2 for ~3% of land parcels. We deliberately do
    # NOT impute a value here (unlike an ML feature pipeline, where a
    # missing value might be filled with a median so a model can still run).
    # In a warehouse used for reporting, an invented number would silently
    # corrupt every average/sum computed over surface_m2. We keep it NULL
    # and let it correctly excluded from aggregate functions (AVG, SUM
    # ignore NULLs in SQL).
    df = df.withColumn(
        "surface_m2_clean",
        F.when(F.col("surface_m2") == "", None).otherwise(F.col("surface_m2").cast("double")),
    )

    # Issue: missing city for ~2% of rows. Same reasoning: kept NULL rather
    # than replaced with a placeholder like "Unknown", which would silently
    # get counted as a real city in group-by queries.
    df = df.withColumn(
        "city_clean",
        F.when(F.col("city") == "", None).otherwise(F.col("city")),
    )

    df = df.withColumn("transaction_date_clean", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))

    return df.select(
        "transaction_id",
        "listing_id",
        "property_type",
        "region",
        F.col("city_clean").alias("city"),
        F.col("surface_m2_clean").alias("surface_m2"),
        F.col("price_mad_clean").alias("price_mad"),
        "registration_status",
        "source_office",
        F.col("transaction_date_clean").alias("transaction_date"),
    )


def build_and_load_dimensions(spark: SparkSession, clean_df):
    """
    Stage 4a: build each dimension table from the distinct values present
    in the cleaned data, and load it into Postgres.

    APPROACH: this is a full (not incremental) load -- appropriate for a
    first historical load. We write only the natural-key columns; Postgres
    generates the surrogate keys (SERIAL) automatically. A production,
    recurring pipeline would need slowly-changing-dimension (SCD) logic to
    handle re-runs without creating duplicate dimension rows -- a natural
    "next step" talking point for the oral defense.
    """
    # dim_date: one row per distinct calendar date, with pre-computed parts
    dim_date = (
        clean_df.select("transaction_date").distinct()
        .withColumn("full_date", F.col("transaction_date"))
        .withColumn("year", F.year("transaction_date"))
        .withColumn("quarter", F.quarter("transaction_date"))
        .withColumn("month", F.month("transaction_date"))
        .withColumn("day_of_week", F.dayofweek("transaction_date"))
        .select("full_date", "year", "quarter", "month", "day_of_week")
    )
    dim_date.write.jdbc(POSTGRES_URL, "dim_date", mode="append", properties=POSTGRES_PROPERTIES)

    dim_property_type = (
        clean_df.select(F.col("property_type").alias("property_type_name")).distinct()
    )
    dim_property_type.write.jdbc(POSTGRES_URL, "dim_property_type", mode="append", properties=POSTGRES_PROPERTIES)

    dim_location = clean_df.select("region", "city").distinct()
    dim_location.write.jdbc(POSTGRES_URL, "dim_location", mode="append", properties=POSTGRES_PROPERTIES)

    dim_registration_status = (
        clean_df.select(F.col("registration_status").alias("status_name")).distinct()
    )
    dim_registration_status.write.jdbc(
        POSTGRES_URL, "dim_registration_status", mode="append", properties=POSTGRES_PROPERTIES
    )

    print("Dimensions loaded: dim_date, dim_property_type, dim_location, dim_registration_status")


def build_and_load_fact(spark: SparkSession, clean_df):
    """
    Stage 4b: build the fact table.

    Since Postgres generated the dimension surrogate keys during the write
    above, we read the dimension tables BACK from Postgres (now that they
    have IDs) and join them against the cleaned data to attach each
    transaction to the right foreign keys.
    """
    dim_date = spark.read.jdbc(POSTGRES_URL, "dim_date", properties=POSTGRES_PROPERTIES)
    dim_property_type = spark.read.jdbc(POSTGRES_URL, "dim_property_type", properties=POSTGRES_PROPERTIES)
    dim_location = spark.read.jdbc(POSTGRES_URL, "dim_location", properties=POSTGRES_PROPERTIES)
    dim_registration_status = spark.read.jdbc(
        POSTGRES_URL, "dim_registration_status", properties=POSTGRES_PROPERTIES
    )

    fact_df = (
        clean_df
        .join(dim_date, clean_df.transaction_date == dim_date.full_date, "left")
        .join(dim_property_type, clean_df.property_type == dim_property_type.property_type_name, "left")
        .join(
            dim_location,
            (clean_df.region == dim_location.region)
            & (clean_df.city.eqNullSafe(dim_location.city)),
            "left",
        )
        .join(
            dim_registration_status,
            clean_df.registration_status == dim_registration_status.status_name,
            "left",
        )
        .select(
            clean_df.transaction_id,
            clean_df.listing_id,
            dim_date.date_id,
            dim_property_type.property_type_id,
            dim_location.location_id,
            dim_registration_status.registration_status_id,
            clean_df.source_office,
            clean_df.surface_m2,
            clean_df.price_mad,
        )
    )

    fact_df.write.jdbc(POSTGRES_URL, "fact_transactions", mode="append", properties=POSTGRES_PROPERTIES)
    print(f"Fact table loaded: {fact_df.count()} transactions")


def main():
    spark = build_spark_session()

    raw_df = extract(spark)
    print(f"Extracted {raw_df.count()} raw rows")

    load_raw_to_lake(raw_df)

    clean_df = transform(raw_df)
    clean_df.cache()  # reused across dimension + fact loading, avoid recomputing the transform each time
    print(f"After deduplication: {clean_df.count()} clean rows")

    build_and_load_dimensions(spark, clean_df)
    build_and_load_fact(spark, clean_df)

    spark.stop()


if __name__ == "__main__":
    main()