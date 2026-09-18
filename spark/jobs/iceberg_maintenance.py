"""
iceberg_maintenance.py — manutencao periodica das tabelas Iceberg.

    expire_snapshots     apaga versoes antigas da tabela (o Iceberg guarda cada
                         escrita como um snapshot; sem expirar, o historico so cresce)
    remove_orphan_files  apaga arquivos no MinIO que nenhum manifesto referencia
    rewrite_manifests    reagrupa os manifestos (o indice de quais arquivos formam a tabela)
    compaction           junta arquivos pequenos em arquivos grandes

A lista de tabelas NAO fica escrita aqui: sai da secao `entidades` de
canonico/modelo_canonico.yaml — a mesma que o `--ddl` do canonico_para_silver.py
usa para cria-las. Assim, tabela nova no modelo entra na manutencao sozinha.

    python3 iceberg_maintenance.py --op expire_snapshots --older-than-days 7
"""
import argparse
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yaml

CATALOGO = "lakehouse"

# O mesmo arquivo visto de dois lugares: a arvore do projeto (execucao no host)
# e o ponto onde o docker-compose o monta dentro dos conteineres.
CAMINHOS_MODELO = [
    Path(__file__).resolve().parents[2] / "canonico" / "modelo_canonico.yaml",
    Path("/opt/canonico/modelo_canonico.yaml"),
]


def get_spark():
    # importado aqui, e nao no topo, para que a lista de tabelas possa ser
    # conferida fora do container, sem Spark instalado
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("iceberg_maintenance")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # O leitor VETORIZADO do Iceberg derruba o executor sem excecao Java
        # (codigo 134). A compactacao rele todos os arquivos de dados: e
        # exatamente a leitura que dispara o defeito. Ver canonico_para_silver.py.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .getOrCreate()
    )


def tabelas_do_modelo():
    """Devolve lakehouse.<camada>.<entidade> para cada entidade do modelo canonico."""
    candidatos = list(CAMINHOS_MODELO)
    if os.environ.get("MODELO_CANONICO"):
        candidatos.insert(0, Path(os.environ["MODELO_CANONICO"]))

    for caminho in candidatos:
        if caminho.exists():
            modelo = yaml.safe_load(caminho.read_text(encoding="utf-8"))
            # minusculas porque e assim que o Hive Metastore guarda os nomes
            return [
                f"{CATALOGO}.{entidade['camada']}.{nome}".lower()
                for nome, entidade in modelo["entidades"].items()
            ]

    raise SystemExit(
        "modelo canonico nao encontrado em:\n  "
        + "\n  ".join(str(c) for c in candidatos)
    )


def _para_cada_tabela(spark, tabelas, op, sql_de):
    """Roda a mesma chamada em todas as tabelas; tabela que falha vira SKIP, nao para a rodada."""
    for tabela in tabelas:
        try:
            spark.sql(sql_de(tabela))
            print(f"{op} OK: {tabela}")
        except Exception as e:
            print(f"{op} SKIP {tabela}: {e}")


def expire_snapshots(spark, tabelas, older_than_days: int):
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    _para_cada_tabela(
        spark, tabelas, "expire_snapshots",
        lambda t: f"""
            CALL {CATALOGO}.system.expire_snapshots(
                table => '{t}',
                older_than => TIMESTAMP '{cutoff_str}'
            )
        """,
    )


def remove_orphan_files(spark, tabelas):
    _para_cada_tabela(
        spark, tabelas, "remove_orphan_files",
        lambda t: f"CALL {CATALOGO}.system.remove_orphan_files(table => '{t}')",
    )


def rewrite_manifests(spark, tabelas):
    _para_cada_tabela(
        spark, tabelas, "rewrite_manifests",
        lambda t: f"CALL {CATALOGO}.system.rewrite_manifests(table => '{t}')",
    )


def compaction(spark, tabelas):
    _para_cada_tabela(
        spark, tabelas, "compaction",
        lambda t: f"CALL {CATALOGO}.system.rewrite_data_files(table => '{t}')",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--op", choices=["expire_snapshots", "remove_orphan_files", "rewrite_manifests", "compaction"], required=True)
    parser.add_argument("--older-than-days", type=int, default=7)
    args = parser.parse_args()

    tabelas = tabelas_do_modelo()
    print(f"{len(tabelas)} tabelas do modelo canonico\n")

    spark = get_spark()

    if args.op == "expire_snapshots":
        expire_snapshots(spark, tabelas, args.older_than_days)
    elif args.op == "remove_orphan_files":
        remove_orphan_files(spark, tabelas)
    elif args.op == "rewrite_manifests":
        rewrite_manifests(spark, tabelas)
    elif args.op == "compaction":
        compaction(spark, tabelas)

    spark.stop()


if __name__ == "__main__":
    main()
