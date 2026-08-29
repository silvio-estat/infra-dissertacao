"""
DAG: Sync Baseline PostgreSQL
Carrega os mesmos dados sintéticos do Lakehouse no PostgreSQL baseline
para comparação de métricas (Indicadores 1-7 do framework GQM).
Os mesmos dados entram nos dois paradigmas — sem vantagem para nenhum.

Estratégia: TRUNCATE + reload completo a cada execução.
Garante dados idênticos nos dois sistemas para comparação justa.

Uso:
  docker exec dlh_airflow_webserver airflow dags trigger dag_baseline_sync
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "dlh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

TRINO_HOST = "trino"
TRINO_PORT = 8090
TRINO_USER = "admin"
BATCH_SIZE = 1000


def _get_pg_conn():
    import os
    import psycopg2

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=os.environ["POSTGRES_DB_BASELINE"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _get_trino_conn():
    import trino

    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
    )


def _truncate_and_reload(pg_table: str, trino_query: str, insert_sql: str):
    """Trunca a tabela PG e recarrega com dados do Trino via batches."""
    from psycopg2.extras import execute_values

    trino_conn = _get_trino_conn()
    pg_conn = _get_pg_conn()

    try:
        trino_cur = trino_conn.cursor()
        pg_cur = pg_conn.cursor()

        pg_cur.execute(f"TRUNCATE TABLE {pg_table} RESTART IDENTITY")

        trino_cur.execute(trino_query)

        total = 0
        while True:
            rows = trino_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            execute_values(pg_cur, insert_sql, rows)
            total += len(rows)

        pg_conn.commit()
        print(f"{pg_table}: {total} linhas sincronizadas")
        return total

    finally:
        trino_conn.close()
        pg_conn.close()


def sync_gps_baseline(**context):
    """Lê Silver GPS (Iceberg/Trino) e insere no PostgreSQL baseline."""
    query = """
        SELECT
            batalhao_origem,
            subunidade,
            latitude,
            longitude,
            altitude,
            velocidade,
            direcao,
            timestamp_geracao,
            timestamp_chegada,
            id_lote
        FROM iceberg.silver.gps
    """
    insert_sql = """
        INSERT INTO gps_posicionamento (
            batalhao_origem, subunidade,
            latitude, longitude, altitude, velocidade, direcao,
            timestamp_geracao, timestamp_chegada, id_lote
        ) VALUES %s
    """
    _truncate_and_reload("gps_posicionamento", query, insert_sql)


def sync_sensor_baseline(**context):
    """Lê Silver Sensor (Iceberg/Trino) e insere no PostgreSQL baseline."""
    query = """
        SELECT
            batalhao_origem,
            drone_id,
            area_cobertura,
            latitude_centro,
            longitude_centro,
            raio_km,
            altitude_voo,
            bateria_pct,
            status_missao,
            timestamp_geracao,
            timestamp_chegada,
            id_lote
        FROM iceberg.silver.sensor
    """
    insert_sql = """
        INSERT INTO sensor_drone (
            batalhao_origem, drone_id,
            area_cobertura, latitude_centro, longitude_centro,
            raio_km, altitude_voo, bateria_pct, status_missao,
            timestamp_geracao, timestamp_chegada, id_lote
        ) VALUES %s
    """
    _truncate_and_reload("sensor_drone", query, insert_sql)


def sync_pessoal_baseline(**context):
    """Lê Silver pessoal (Iceberg/Trino) e insere no PostgreSQL baseline."""
    query = """
        SELECT
            id_relatorio,
            batalhao_origem,
            subunidade,
            situacao_operacional,
            efetivo_organico,
            efetivo_presente,
            baixas_combate,
            baixas_nao_combate,
            evacuados,
            necessidade_prioritaria,
            necessidade_logistica,
            timestamp_geracao,
            timestamp_chegada,
            id_lote
        FROM iceberg.silver.pessoal
    """
    insert_sql = """
        INSERT INTO pessoal_subunidade (
            id_relatorio, batalhao_origem, subunidade,
            situacao_operacional,
            efetivo_organico, efetivo_presente,
            baixas_combate, baixas_nao_combate, evacuados,
            necessidade_prioritaria, necessidade_logistica,
            timestamp_geracao, timestamp_chegada, id_lote
        ) VALUES %s
    """
    _truncate_and_reload("pessoal_subunidade", query, insert_sql)


def sync_material_baseline(**context):
    """Lê Silver material (Iceberg/Trino) e insere no PostgreSQL baseline."""
    query = """
        SELECT
            id_viatura,
            batalhao_origem,
            subunidade,
            tipo_viatura,
            status_viatura,
            nivel_combustivel_pct,
            km_rodados,
            proxima_manutencao_km,
            timestamp_geracao,
            timestamp_chegada,
            id_lote
        FROM iceberg.silver.material
    """
    insert_sql = """
        INSERT INTO material_viatura (
            id_viatura, batalhao_origem, subunidade,
            tipo_viatura, status_viatura,
            nivel_combustivel_pct, km_rodados, proxima_manutencao_km,
            timestamp_geracao, timestamp_chegada, id_lote
        ) VALUES %s
    """
    _truncate_and_reload("material_viatura", query, insert_sql)


with DAG(
    dag_id="dag_baseline_sync",
    description="Sincroniza dados Silver → PostgreSQL baseline para comparação paradigmática",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    default_args=default_args,
    tags=["baseline", "postgresql", "benchmark"],
) as dag:

    sync_gps = PythonOperator(
        task_id="sync_gps_postgres",
        python_callable=sync_gps_baseline,
    )

    sync_sensor = PythonOperator(
        task_id="sync_sensor_postgres",
        python_callable=sync_sensor_baseline,
    )

    sync_pessoal = PythonOperator(
        task_id="sync_pessoal_postgres",
        python_callable=sync_pessoal_baseline,
    )

    sync_material = PythonOperator(
        task_id="sync_material_postgres",
        python_callable=sync_material_baseline,
    )

    [sync_gps, sync_sensor, sync_pessoal, sync_material]
