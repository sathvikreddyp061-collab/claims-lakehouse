"""Bronze ingest — claims.edi837.v1 Kafka topic → Iceberg bronze.claims_edi837.

Reads the JSON-wrapped EDI 837 events, persists the raw envelope and a small
set of decoded header fields (claim_id, subscriber_id, service_date,
total_charge) for fast filtering. Full EDI parsing into structured tables
happens in dbt staging (`stg_claims_837`).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "redpanda:9092")
TOPIC = os.getenv("TOPIC_CLAIM", "claims.edi837.v1")
S3_ENDPOINT = os.getenv("S3_ENDPOINT_INTERNAL", "http://minio:9000")
S3_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse")

CHECKPOINT = "s3a://checkpoints/bronze/claims_edi837"
BRONZE_TABLE = "lakehouse.bronze.claims_edi837"


PAYLOAD_SCHEMA = StructType([
    StructField("claim_id", StringType()),
    StructField("subscriber_id", StringType()),
    StructField("receiver_id", StringType()),
    StructField("service_date", StringType()),
    StructField("total_charge", StringType()),
    StructField("edi_837", StringType()),
    StructField("ingest_ts", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("claims-bronze-ingest")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            claim_id        STRING,
            subscriber_id   STRING,
            receiver_id     STRING,
            service_date    DATE,
            total_charge    DECIMAL(12, 2),
            edi_837         STRING,
            ingest_ts       TIMESTAMP,
            kafka_partition INT,
            kafka_offset    LONG,
            service_month   DATE
        )
        USING iceberg
        PARTITIONED BY (months(service_month))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '134217728'
        )
    """)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("failOnDataLoss", "false")
        .load()
    )

    decoded = (
        raw
        .select(
            F.from_json(F.col("value").cast("string"), PAYLOAD_SCHEMA).alias("p"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select("p.*", "kafka_partition", "kafka_offset")
        .withColumn("service_date", F.to_date("service_date"))
        .withColumn("ingest_ts", F.to_timestamp("ingest_ts"))
        .withColumn("total_charge", F.col("total_charge").cast("decimal(12, 2)"))
        .withColumn("service_month", F.date_trunc("month", F.col("service_date")).cast("date"))
    )

    query = (
        decoded.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .toTable(BRONZE_TABLE)
    )
    print(f"[bronze] streaming → {BRONZE_TABLE}  checkpoint={CHECKPOINT}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
