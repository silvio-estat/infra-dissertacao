"""
Bronze Ingestor — Spark Job
Lê arquivos JSON do bucket 'landing' no MinIO,
adiciona metadados de proveniência e persiste como tabela Iceberg na camada Bronze.
"""
import argparse
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType


def get_spark():
    return (
        SparkSession.builder
        .appName("bronze_ingestor")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def _comentar(spark: SparkSession, tabela: str, comentarios: dict):
    for coluna, texto in comentarios.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{texto}'")


def _comentar_tabela(spark: SparkSession, tabela: str, descricao: str):
    spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES ('comment' = '{descricao}')")


def criar_tabela_bronze_se_necessario(spark: SparkSession):
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS lakehouse.bronze
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.dados (
            id_registro         STRING,
            batalhao_origem     STRING,
            tipo_dado           STRING,
            timestamp_geracao   TIMESTAMP,
            timestamp_chegada   TIMESTAMP,
            id_lote             STRING,
            payload             STRING,
            fonte_arquivo       STRING
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
        TBLPROPERTIES (
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='snappy'
        )
    """)


def comentar_bronze(spark: SparkSession):
    _comentar_tabela(spark, "lakehouse.bronze.dados",
        "Zona de aterrissagem dos dados brutos transmitidos pelos batalhoes subordinados. "
        "Append-only — nunca sofre DELETE ou UPDATE. Preserva o payload original intacto "
        "como evidencia de auditabilidade. Os 8 tipos de dado (gps, sensor, relt_intel, paf, "
        "obstaculo, seg_area, pessoal, material) coexistem separados pelo campo tipo_dado, "
        "particionados por dia de chegada e batalhao_origem."
    )
    _comentar(spark, "lakehouse.bronze.dados", {
        "id_registro":       "UUID gerado na ingestao — identifica unicamente o registro fisico na Bronze, independente do tipo de dado",
        "batalhao_origem":   "Sigla do batalhao que originou o dado (ex: 1BPE, 2BIB)",
        "tipo_dado":         "Tipo de payload: gps, sensor, relt_intel, paf, obstaculo, seg_area, pessoal, material",
        "timestamp_geracao": "Momento em que o batalhao gerou o dado (relogio do sistema de origem)",
        "timestamp_chegada": "Momento em que o dado chegou a camada Bronze (relogio do servidor de ingestao) — diferenca em relacao a timestamp_geracao e a latencia de transmissao",
        "id_lote":           "Identificador do batch de ingestao — agrupa todos os registros processados no mesmo job Spark",
        "payload":           "JSON completo do registro original, exatamente como recebido, sem qualquer modificacao ou filtragem",
        "fonte_arquivo":     "Caminho do arquivo JSON no MinIO (bucket landing/) de onde o registro foi lido",
    })


def ingerir(spark: SparkSession, logical_date: str):
    timestamp_chegada = datetime.now(timezone.utc)
    id_lote = f"lote_{timestamp_chegada.strftime('%Y%m%d_%H%M%S')}"

    # Lê todos os JSONs disponíveis no bucket landing (incluindo subdiretórios tipo/lote_*.json)
    try:
        df_raw = spark.read.option("multiline", "true").option("recursiveFileLookup", "true").json("s3a://landing/")
    except Exception as e:
        print(f"Nenhum arquivo no landing ou erro de leitura: {e}")
        return

    if df_raw.rdd.isEmpty():
        print("Landing vazio — nenhum dado para ingerir.")
        return

    # Adiciona metadados de proveniência obrigatórios (seção 3.2 do relatório)
    df_bronze = (
        df_raw
        .withColumn("id_registro", F.expr("uuid()"))
        .withColumn("timestamp_chegada", F.lit(timestamp_chegada).cast(TimestampType()))
        .withColumn("id_lote", F.lit(id_lote))
        .withColumn("payload", F.to_json(F.struct([F.col(c) for c in df_raw.columns])))
        .select(
            F.col("id_registro"),
            F.col("batalhao_origem").cast(StringType()),
            F.col("tipo_dado").cast(StringType()),
            F.col("timestamp_geracao").cast(TimestampType()),
            F.col("timestamp_chegada"),
            F.col("id_lote"),
            F.col("payload"),
            F.input_file_name().alias("fonte_arquivo"),
        )
    )

    # Append-only — Bronze nunca deleta dados
    df_bronze.writeTo("lakehouse.bronze.dados").append()

    count = df_bronze.count()
    print(f"Bronze: {count} registros ingeridos no lote {id_lote}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logical-date", default=datetime.now(timezone.utc).isoformat())
    args = parser.parse_args()

    spark = get_spark()
    criar_tabela_bronze_se_necessario(spark)
    comentar_bronze(spark)
    ingerir(spark, args.logical_date)
    spark.stop()


if __name__ == "__main__":
    main()
