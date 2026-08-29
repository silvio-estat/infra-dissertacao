"""
Iceberg Maintenance — Spark Job
expire_snapshots, remove_orphan_files, rewrite_manifests, compaction.
"""
import argparse
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession


def get_spark():
    return (
        SparkSession.builder
        .appName("iceberg_maintenance")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


TABELAS = [
    "lakehouse.bronze.dados",
    "lakehouse.silver.gps",
    "lakehouse.silver.sensor",
    "lakehouse.silver.relt_intel",
    "lakehouse.silver.paf",
    "lakehouse.silver.obstaculo",
    "lakehouse.silver.seg_area",
    "lakehouse.silver.pessoal",
    "lakehouse.silver.material",
    "lakehouse.gold.coc",
    "lakehouse.gold.pitcic",
    "lakehouse.gold.ppcot",
    "lakehouse.gold.avaliacao",
]


def expire_snapshots(spark: SparkSession, older_than_days: int):
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    for tabela in TABELAS:
        try:
            spark.sql(f"""
                CALL lakehouse.system.expire_snapshots(
                    table => '{tabela}',
                    older_than => TIMESTAMP '{cutoff_str}'
                )
            """)
            print(f"expire_snapshots OK: {tabela}")
        except Exception as e:
            print(f"expire_snapshots SKIP {tabela}: {e}")


def remove_orphan_files(spark: SparkSession):
    for tabela in TABELAS:
        try:
            spark.sql(f"""
                CALL lakehouse.system.remove_orphan_files(table => '{tabela}')
            """)
            print(f"remove_orphan_files OK: {tabela}")
        except Exception as e:
            print(f"remove_orphan_files SKIP {tabela}: {e}")


def rewrite_manifests(spark: SparkSession):
    for tabela in TABELAS:
        try:
            spark.sql(f"""
                CALL lakehouse.system.rewrite_manifests(table => '{tabela}')
            """)
            print(f"rewrite_manifests OK: {tabela}")
        except Exception as e:
            print(f"rewrite_manifests SKIP {tabela}: {e}")


def compaction(spark: SparkSession):
    for tabela in TABELAS:
        try:
            spark.sql(f"""
                CALL lakehouse.system.rewrite_data_files(table => '{tabela}')
            """)
            print(f"compaction OK: {tabela}")
        except Exception as e:
            print(f"compaction SKIP {tabela}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--op", choices=["expire_snapshots", "remove_orphan_files", "rewrite_manifests", "compaction"], required=True)
    parser.add_argument("--older-than-days", type=int, default=7)
    args = parser.parse_args()

    spark = get_spark()

    if args.op == "expire_snapshots":
        expire_snapshots(spark, args.older_than_days)
    elif args.op == "remove_orphan_files":
        remove_orphan_files(spark)
    elif args.op == "rewrite_manifests":
        rewrite_manifests(spark)
    elif args.op == "compaction":
        compaction(spark)

    spark.stop()


if __name__ == "__main__":
    main()
