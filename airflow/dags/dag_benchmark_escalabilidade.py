"""
DAG: Benchmark de Escalabilidade — Lakehouse vs PostgreSQL
Objetivo: encontrar o ponto de cruzamento (crossover) onde Spark+Iceberg
supera PostgreSQL nas queries analíticas GQM.

Trigger manual com parâmetro de volume:
  airflow dags trigger dag_benchmark_escalabilidade --conf '{"registros_por_tipo": 50000}'

Fluxo:
  1. Pausa DAGs operacionais (evita interferência)
  2. Gera N registros (GPS + SITREP + Sensor) no landing
  3. Executa pipeline completo: Bronze → Silver → Gold → Baseline Sync
  4. Conta registros em ambos os paradigmas
  5. Executa queries GQM benchmark (5 repetições cada)
  6. Grava resultados na tabela benchmark_resultados (PostgreSQL)

Nota: os dados se ACUMULAM entre rodadas — cada execução adiciona volume
e mede no total acumulado, simulando crescimento real.
"""
from __future__ import annotations

import os
import time
import statistics
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "dlh",
    "retries": 0,
    "email_on_failure": False,
}

DAGS_OPERACIONAIS = [
    "dag_ingestao_bronze",
    "dag_silver_transform",
    "dag_gold_refresh",
    "dag_baseline_sync",
    "dag_iceberg_maintenance",
]

TRINO_HOST = "trino"
TRINO_PORT = 8090
TRINO_USER = "admin"

REPETICOES_QUERY = 5

SPARK_CONF_BASE = {
    "spark.cores.max": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "4",
    "spark.default.parallelism": "4",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.catalog.lakehouse.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.lakehouse.warehouse": "s3a://lakehouse/warehouse",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", ""),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        database=os.environ["POSTGRES_DB_BASELINE"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _get_trino_conn():
    import trino as trino_lib
    return trino_lib.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, http_scheme="http",
    )


def _medir_query(conn_factory, sql, repeticoes=REPETICOES_QUERY):
    """Executa query N vezes, retorna (media_ms, desvio_ms, min_ms, max_ms)."""
    tempos = []
    for _ in range(repeticoes):
        conn = conn_factory()
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        tempos.append(elapsed_ms)
        conn.close()
    media = statistics.mean(tempos)
    desvio = statistics.stdev(tempos) if len(tempos) > 1 else 0
    return media, desvio, min(tempos), max(tempos)


# ── Queries GQM para benchmark ───────────────────────────────────────────────

QUERIES_TRINO = {
    "Q1_posicao_atual": """
        SELECT batalhao_origem, subunidade, latitude, longitude,
               timestamp_geracao, id_lote
        FROM iceberg.gold.posicionamento_atual
        ORDER BY batalhao_origem, subunidade
    """,
    "Q2_pessoal_4h": """
        SELECT batalhao_origem, count(*) AS total,
               SUM(baixas_combate + baixas_nao_combate) AS baixas, MAX(timestamp_geracao) AS ultimo
        FROM iceberg.silver.pessoal
        WHERE timestamp_geracao >= (
            SELECT MAX(timestamp_geracao) - INTERVAL '4' HOUR
            FROM iceberg.silver.pessoal
        )
        GROUP BY batalhao_origem
        ORDER BY batalhao_origem
    """,
    "Q3_fusao_multifonte": """
        WITH janela AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem,
                   AVG(latitude) AS lat_media, AVG(longitude) AS lon_media,
                   count(*) AS registros_gps
            FROM iceberg.silver.gps GROUP BY 1, 2
        ),
        sit AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem,
                   count(*) AS registros_pessoal,
                   SUM(baixas_combate + baixas_nao_combate) AS baixas
            FROM iceberg.silver.pessoal GROUP BY 1, 2
        ),
        sen AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem, count(*) AS registros_sensor
            FROM iceberg.silver.sensor GROUP BY 1, 2
        )
        SELECT j.batalhao_origem, j.hora,
               j.registros_gps, j.lat_media, j.lon_media,
               COALESCE(s.registros_pessoal, 0), COALESCE(s.baixas, 0),
               COALESCE(d.registros_sensor, 0)
        FROM janela j
        LEFT JOIN sit s ON j.batalhao_origem = s.batalhao_origem AND j.hora = s.hora
        LEFT JOIN sen d ON j.batalhao_origem = d.batalhao_origem AND j.hora = d.hora
        ORDER BY j.batalhao_origem, j.hora
    """,
    "Q4_latencia_percentis": """
        SELECT batalhao_origem, latencia_media_s, p50_s, p90_s, p99_s
        FROM iceberg.gold.latencia_por_batalhao
        ORDER BY batalhao_origem
    """,
    "Q5_gaps_cobertura": """
        SELECT batalhao_origem, subunidade, inicio_gap, fim_gap, gap_minutos
        FROM iceberg.gold.cobertura_temporal
        ORDER BY gap_minutos DESC
    """,
}

QUERIES_PG = {
    "Q1_posicao_atual": """
        SELECT batalhao_origem, subunidade, latitude, longitude,
               timestamp_geracao, id_lote
        FROM v_posicionamento_atual
        ORDER BY batalhao_origem, subunidade
    """,
    "Q2_pessoal_4h": """
        SELECT batalhao_origem, count(*) AS total,
               SUM(baixas_combate + baixas_nao_combate) AS baixas, MAX(timestamp_geracao) AS ultimo
        FROM pessoal_subunidade
        WHERE timestamp_geracao >= (
            SELECT MAX(timestamp_geracao) - INTERVAL '4 hours' FROM pessoal_subunidade
        )
        GROUP BY batalhao_origem
        ORDER BY batalhao_origem
    """,
    "Q3_fusao_multifonte": """
        WITH janela AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem,
                   AVG(latitude) AS lat_media, AVG(longitude) AS lon_media,
                   count(*) AS registros_gps
            FROM gps_posicionamento GROUP BY 1, 2
        ),
        sit AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem,
                   count(*) AS registros_pessoal,
                   SUM(baixas_combate + baixas_nao_combate) AS baixas
            FROM pessoal_subunidade GROUP BY 1, 2
        ),
        sen AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem, count(*) AS registros_sensor
            FROM sensor_drone GROUP BY 1, 2
        )
        SELECT j.batalhao_origem, j.hora,
               j.registros_gps, j.lat_media, j.lon_media,
               COALESCE(s.registros_pessoal, 0), COALESCE(s.baixas, 0),
               COALESCE(d.registros_sensor, 0)
        FROM janela j
        LEFT JOIN sit s ON j.batalhao_origem = s.batalhao_origem AND j.hora = s.hora
        LEFT JOIN sen d ON j.batalhao_origem = d.batalhao_origem AND j.hora = d.hora
        ORDER BY j.batalhao_origem, j.hora
    """,
    "Q4_latencia_percentis": """
        SELECT batalhao_origem,
               AVG(EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS media_s,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS p50_s,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS p90_s,
               PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS p99_s
        FROM (
            SELECT batalhao_origem, timestamp_geracao, timestamp_chegada FROM gps_posicionamento
            UNION ALL
            SELECT batalhao_origem, timestamp_geracao, timestamp_chegada FROM pessoal_subunidade
            UNION ALL
            SELECT batalhao_origem, timestamp_geracao, timestamp_chegada FROM sensor_drone
        ) dados
        GROUP BY batalhao_origem
        ORDER BY batalhao_origem
    """,
    "Q5_gaps_cobertura": """
        SELECT batalhao_origem, subunidade,
               LAG(timestamp_geracao) OVER w AS inicio_gap,
               timestamp_geracao AS fim_gap,
               EXTRACT(EPOCH FROM (timestamp_geracao - LAG(timestamp_geracao) OVER w)) / 60 AS gap_minutos
        FROM gps_posicionamento
        WINDOW w AS (PARTITION BY batalhao_origem, subunidade ORDER BY timestamp_geracao)
        ORDER BY gap_minutos DESC NULLS LAST
        LIMIT 50
    """,
}


# ── Tasks ────────────────────────────────────────────────────────────────────

def pausar_dags_operacionais(**context):
    """Pausa DAGs operacionais para evitar interferência nas medições."""
    from airflow.models import DagModel
    from airflow.utils.session import create_session

    with create_session() as session:
        for dag_id in DAGS_OPERACIONAIS:
            dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if dag_model:
                dag_model.is_paused = True
                print(f"  Pausada: {dag_id}")
            else:
                print(f"  Não encontrada: {dag_id}")
        session.commit()
    print("DAGs operacionais pausadas.")


def gerar_dados_benchmark(**context):
    """Gera registros sintéticos no landing bucket."""
    import json
    import random
    from io import BytesIO
    import boto3
    from botocore.client import Config

    conf = context["dag_run"].conf or {}
    registros_por_tipo = conf.get("registros_por_tipo", 10000)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD"),
        config=Config(signature_version="s3v4"),
    )

    from datetime import timezone
    BATALHOES = ["1BPE", "2BPE", "3BPE", "4BPE", "5BPE", "1BIB", "2BIB"]
    SUBUNIDADES = {
        "1BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
        "2BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
        "3BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
        "4BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
        "5BPE": ["Cia Cmdo", "1a Cia PE", "2a Cia PE", "3a Cia PE"],
        "1BIB": ["Cia Cmdo", "1a Cia Fuz Bld", "2a Cia Fuz Bld", "3a Cia Fuz Bld", "Cia Ap"],
        "2BIB": ["Cia Cmdo", "1a Cia Fuz Bld", "2a Cia Fuz Bld", "3a Cia Fuz Bld", "Cia Ap"],
    }
    LAT_BASE, LON_BASE = -15.77, -47.92

    def ts_recente(minutos=480):
        delta = timedelta(minutes=random.randint(0, minutos))
        return (datetime.now(timezone.utc) - delta).isoformat()

    # Gera GPS
    registros_gps = []
    for _ in range(registros_por_tipo):
        bat = random.choice(BATALHOES)
        sub = random.choice(SUBUNIDADES[bat])
        registros_gps.append({
            "batalhao_origem": bat, "tipo_dado": "gps",
            "timestamp_geracao": ts_recente(),
            "id_veiculo": f"VTR-{bat}-{random.randint(1,4):02d}-{random.randint(1,25):02d}",
            "subunidade": sub,
            "latitude": round(LAT_BASE + random.uniform(-2, 2), 6),
            "longitude": round(LON_BASE + random.uniform(-3, 3), 6),
            "altitude_m": round(random.uniform(800, 1200), 1),
            "velocidade_kmh": round(random.uniform(0, 120), 1),
            "direcao_graus": random.randint(0, 359),
            "precisao_m": round(random.uniform(3, 50), 1),
        })

    # Gera Pessoal (substitui sitrep como terceiro tipo de dado)
    situacoes = ["OPERACIONAL", "DEGRADADO", "INOPERANTE", "RESERVA"]
    necessidades_s1 = ["PESSOAL_REFORCADO", "EVACUACAO_MEDICA", "NENHUMA"]
    necessidades_log = ["MUNICAO", "COMBUSTIVEL", "RACOES", "MATERIAL SAUDE", "NENHUMA"]
    registros_pessoal = []
    for _ in range(registros_por_tipo):
        bat = random.choice(BATALHOES)
        sub = random.choice(SUBUNIDADES[bat])
        efetivo_organico = 120
        baixas_combate     = random.choices([0, 0, 0, 0, 1, 2, 3], k=1)[0]
        baixas_nao_combate = random.choices([0, 0, 1, 2], k=1)[0]
        evacuados          = random.choices([0, 0, 0, 1], k=1)[0]
        registros_pessoal.append({
            "batalhao_origem":         bat,
            "tipo_dado":               "pessoal",
            "timestamp_geracao":       ts_recente(480),
            "id_relatorio":            str(__import__("uuid").uuid4()),
            "subunidade":              sub,
            "situacao_operacional":    random.choice(situacoes),
            "efetivo_organico":        efetivo_organico,
            "efetivo_presente":        max(20, efetivo_organico - baixas_combate - baixas_nao_combate - evacuados),
            "baixas_combate":          baixas_combate,
            "baixas_nao_combate":      baixas_nao_combate,
            "evacuados":               evacuados,
            "necessidade_prioritaria": random.choice(necessidades_s1),
            "necessidade_logistica":   random.choice(necessidades_log),
        })

    # Gera Sensor
    registros_sensor = []
    status_opcoes = ["ativo", "retornando", "em_espera", "manutencao"]
    areas = ["NORTE", "SUL", "LESTE", "OESTE", "CENTRO"]
    for _ in range(registros_por_tipo):
        bat = random.choice(BATALHOES)
        registros_sensor.append({
            "batalhao_origem": bat, "tipo_dado": "sensor",
            "timestamp_geracao": ts_recente(120),
            "id_sensor": f"DRN-{bat}-{random.randint(1,15):02d}",
            "drone_id": f"DRN-{bat}-{random.randint(1,15):02d}",
            "area_cobertura": random.choice(areas),
            "latitude_centro": round(LAT_BASE + random.uniform(-2, 2), 6),
            "longitude_centro": round(LON_BASE + random.uniform(-3, 3), 6),
            "raio_km": round(random.uniform(1, 15), 1),
            "altitude_voo": round(random.uniform(50, 500), 0),
            "bateria_pct": random.randint(10, 100),
            "status_missao": random.choice(status_opcoes),
        })

    # Upload em lotes de 5000 para não criar JSONs gigantes
    LOTE_SIZE = 5000
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def upload_lotes(registros, tipo):
        for i in range(0, len(registros), LOTE_SIZE):
            lote = registros[i:i + LOTE_SIZE]
            nome = f"{tipo}/bench_{ts_str}_{i // LOTE_SIZE:04d}.json"
            dados = json.dumps(lote, ensure_ascii=False).encode("utf-8")
            s3.put_object(Bucket="landing", Key=nome, Body=dados,
                          ContentType="application/json")
        print(f"  {tipo}: {len(registros)} registros enviados ao landing")

    upload_lotes(registros_gps, "gps")
    upload_lotes(registros_pessoal, "pessoal")
    upload_lotes(registros_sensor, "sensor")

    total = registros_por_tipo * 3
    print(f"Total gerado nesta rodada: {total} registros ({registros_por_tipo} por tipo)")
    context["ti"].xcom_push(key="registros_gerados", value=total)


def contar_registros(**context):
    """Conta registros em ambos os paradigmas após o pipeline."""
    import trino as trino_lib
    import psycopg2

    trino_c = _get_trino_conn()
    cur = trino_c.cursor()

    contagens = {}
    for tabela in ["iceberg.bronze.dados", "iceberg.silver.gps",
                   "iceberg.silver.pessoal", "iceberg.silver.sensor"]:
        cur.execute(f"SELECT count(*) FROM {tabela}")
        contagens[tabela] = cur.fetchone()[0]
    trino_c.close()

    pg_c = _get_pg_conn()
    pg_cur = pg_c.cursor()
    for tabela in ["gps_posicionamento", "pessoal_subunidade", "sensor_drone"]:
        pg_cur.execute(f"SELECT count(*) FROM {tabela}")
        contagens[f"pg.{tabela}"] = pg_cur.fetchone()[0]
    pg_c.close()

    volume_total = (contagens.get("iceberg.silver.gps", 0) +
                    contagens.get("iceberg.silver.pessoal", 0) +
                    contagens.get("iceberg.silver.sensor", 0))

    print(f"Volume total Silver (Lakehouse): {volume_total}")
    print(f"Contagens detalhadas: {contagens}")

    context["ti"].xcom_push(key="volume_total", value=volume_total)
    context["ti"].xcom_push(key="contagens", value=contagens)


def criar_tabela_resultados(**context):
    """Garante que a tabela de resultados existe no PostgreSQL."""
    pg_c = _get_pg_conn()
    cur = pg_c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_resultados (
            id              SERIAL PRIMARY KEY,
            rodada          INTEGER NOT NULL,
            volume_total    INTEGER NOT NULL,
            query_id        VARCHAR(50) NOT NULL,
            tempo_trino_ms  NUMERIC(12, 3),
            desvio_trino_ms NUMERIC(12, 3),
            min_trino_ms    NUMERIC(12, 3),
            max_trino_ms    NUMERIC(12, 3),
            tempo_pg_ms     NUMERIC(12, 3),
            desvio_pg_ms    NUMERIC(12, 3),
            min_pg_ms       NUMERIC(12, 3),
            max_pg_ms       NUMERIC(12, 3),
            vencedor        VARCHAR(20),
            razao           NUMERIC(8, 3),
            repeticoes      INTEGER DEFAULT 5,
            executado_em    TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_bench_volume
            ON benchmark_resultados(volume_total);
        CREATE INDEX IF NOT EXISTS idx_bench_query
            ON benchmark_resultados(query_id, volume_total);
    """)
    pg_c.commit()
    pg_c.close()
    print("Tabela benchmark_resultados pronta.")


def executar_benchmark(**context):
    """Executa as queries GQM em ambos os paradigmas e grava resultados."""
    import psycopg2

    volume_total = context["ti"].xcom_pull(key="volume_total", task_ids="contar_registros")

    # Determinar número da rodada
    pg_c = _get_pg_conn()
    cur = pg_c.cursor()
    cur.execute("SELECT COALESCE(MAX(rodada), 0) + 1 FROM benchmark_resultados")
    rodada = cur.fetchone()[0]
    pg_c.close()

    print(f"\n{'=' * 60}")
    print(f"  BENCHMARK RODADA {rodada} — Volume: {volume_total} registros")
    print(f"  {REPETICOES_QUERY} repetições por query")
    print(f"{'=' * 60}\n")

    resultados = []

    for query_id in QUERIES_TRINO:
        print(f"  Medindo {query_id}...", end=" ", flush=True)

        media_t, desvio_t, min_t, max_t = _medir_query(
            _get_trino_conn, QUERIES_TRINO[query_id])
        media_p, desvio_p, min_p, max_p = _medir_query(
            _get_pg_conn, QUERIES_PG[query_id])

        vencedor = "trino" if media_t < media_p else "postgresql"
        razao = media_p / media_t if media_t > 0 else 0

        resultados.append({
            "rodada": rodada, "volume_total": volume_total,
            "query_id": query_id,
            "tempo_trino_ms": media_t, "desvio_trino_ms": desvio_t,
            "min_trino_ms": min_t, "max_trino_ms": max_t,
            "tempo_pg_ms": media_p, "desvio_pg_ms": desvio_p,
            "min_pg_ms": min_p, "max_pg_ms": max_p,
            "vencedor": vencedor, "razao": razao,
        })
        print(f"Trino={media_t:.1f}ms  PG={media_p:.1f}ms  -> {vencedor} ({razao:.2f}x)")

    # Gravar no PostgreSQL
    pg_c = _get_pg_conn()
    cur = pg_c.cursor()
    for r in resultados:
        cur.execute("""
            INSERT INTO benchmark_resultados
                (rodada, volume_total, query_id,
                 tempo_trino_ms, desvio_trino_ms, min_trino_ms, max_trino_ms,
                 tempo_pg_ms, desvio_pg_ms, min_pg_ms, max_pg_ms,
                 vencedor, razao, repeticoes)
            VALUES (%(rodada)s, %(volume_total)s, %(query_id)s,
                    %(tempo_trino_ms)s, %(desvio_trino_ms)s, %(min_trino_ms)s, %(max_trino_ms)s,
                    %(tempo_pg_ms)s, %(desvio_pg_ms)s, %(min_pg_ms)s, %(max_pg_ms)s,
                    %(vencedor)s, %(razao)s, %(repeticoes)s)
        """, {**r, "repeticoes": REPETICOES_QUERY})
    pg_c.commit()
    pg_c.close()

    print(f"\nRodada {rodada} concluída — {len(resultados)} medições gravadas.")
    print("Consulte: SELECT * FROM benchmark_resultados ORDER BY rodada, query_id;")


# ── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_benchmark_escalabilidade",
    description="Benchmark progressivo Lakehouse vs PostgreSQL — busca crossover point",
    schedule=None,  # trigger manual apenas
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["benchmark", "gqm", "escalabilidade"],
    params={
        "registros_por_tipo": 10000,
    },
) as dag:

    t_pausar = PythonOperator(
        task_id="pausar_dags_operacionais",
        python_callable=pausar_dags_operacionais,
    )

    t_gerar = PythonOperator(
        task_id="gerar_dados_benchmark",
        python_callable=gerar_dados_benchmark,
    )

    t_bronze = SparkSubmitOperator(
        task_id="pipeline_bronze",
        application="/opt/spark-jobs/bronze_ingestor.py",
        conn_id="spark_default",
        name="bench_bronze",
        conf=SPARK_CONF_BASE,
        application_args=["--logical-date", "{{ logical_date.isoformat() }}"],
        verbose=False,
    )

    def pipeline_silver_trino(**context):
        """Silver transform via Trino (INSERT ... WHERE NOT EXISTS) — sem OOM."""
        trino_c = _get_trino_conn()
        cur = trino_c.cursor()

        # SENSOR
        cur.execute("""
            INSERT INTO iceberg.silver.sensor
            SELECT
                json_extract_scalar(payload, '$.id_sensor'),
                batalhao_origem,
                json_extract_scalar(payload, '$.drone_id'),
                json_extract_scalar(payload, '$.area_cobertura'),
                CAST(json_extract_scalar(payload, '$.latitude_centro') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.longitude_centro') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.raio_km') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.altitude_voo') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.bateria_pct') AS INTEGER),
                json_extract_scalar(payload, '$.status_missao'),
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                to_unixtime(timestamp_chegada) - to_unixtime(timestamp_geracao),
                timestamp_geracao > timestamp_chegada,
                current_timestamp
            FROM iceberg.bronze.dados src
            WHERE tipo_dado = 'sensor'
              AND NOT EXISTS (
                SELECT 1 FROM iceberg.silver.sensor tgt
                WHERE tgt.id_registro = json_extract_scalar(src.payload, '$.id_sensor')
              )
        """)
        print("Silver SENSOR concluído via Trino")

        # GPS — usa id_veiculo como id_registro
        cur.execute("""
            INSERT INTO iceberg.silver.gps
            SELECT
                json_extract_scalar(payload, '$.id_veiculo'),
                batalhao_origem,
                json_extract_scalar(payload, '$.subunidade'),
                CAST(json_extract_scalar(payload, '$.latitude') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.longitude') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.altitude_m') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.velocidade_kmh') AS DOUBLE),
                CAST(json_extract_scalar(payload, '$.direcao_graus') AS DOUBLE),
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                to_unixtime(timestamp_chegada) - to_unixtime(timestamp_geracao),
                timestamp_geracao > timestamp_chegada,
                current_timestamp
            FROM iceberg.bronze.dados src
            WHERE tipo_dado = 'gps'
              AND CAST(json_extract_scalar(payload, '$.latitude') AS DOUBLE) BETWEEN -90 AND 90
              AND CAST(json_extract_scalar(payload, '$.longitude') AS DOUBLE) BETWEEN -180 AND 180
              AND NOT EXISTS (
                SELECT 1 FROM iceberg.silver.gps tgt
                WHERE tgt.id_registro = json_extract_scalar(src.payload, '$.id_veiculo')
                  AND tgt.timestamp_geracao = src.timestamp_geracao
              )
        """)
        print("Silver GPS concluído via Trino")
        trino_c.close()

    t_silver = PythonOperator(
        task_id="pipeline_silver_trino",
        python_callable=pipeline_silver_trino,
    )

    def pipeline_gold_trino(**context):
        """Gold refresh via Trino — DROP + CTAS das visões analíticas."""
        trino_c = _get_trino_conn()
        cur = trino_c.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")

        # Posicionamento atual
        cur.execute("DROP TABLE IF EXISTS iceberg.gold.posicionamento_atual")
        cur.execute("""
            CREATE TABLE iceberg.gold.posicionamento_atual AS
            SELECT batalhao_origem, subunidade, latitude, longitude,
                   altitude, velocidade, direcao, timestamp_geracao,
                   timestamp_chegada, id_lote
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC
                ) AS rn
                FROM iceberg.silver.gps
            ) WHERE rn = 1
        """)
        print("Gold posicionamento_atual OK")

        # Latência por batalhão
        cur.execute("DROP TABLE IF EXISTS iceberg.gold.latencia_por_batalhao")
        cur.execute("""
            CREATE TABLE iceberg.gold.latencia_por_batalhao AS
            SELECT batalhao_origem,
                   avg(latencia_ingestao_s) AS latencia_media_s,
                   approx_percentile(latencia_ingestao_s, 0.5) AS p50_s,
                   approx_percentile(latencia_ingestao_s, 0.9) AS p90_s,
                   approx_percentile(latencia_ingestao_s, 0.99) AS p99_s,
                   count(*) AS total_registros
            FROM (
                SELECT batalhao_origem, latencia_ingestao_s FROM iceberg.silver.gps
                UNION ALL
                SELECT batalhao_origem, latencia_ingestao_s FROM iceberg.silver.pessoal
                UNION ALL
                SELECT batalhao_origem, latencia_ingestao_s FROM iceberg.silver.sensor
            )
            GROUP BY batalhao_origem
        """)
        print("Gold latencia_por_batalhao OK")

        # Cobertura temporal (gaps)
        cur.execute("DROP TABLE IF EXISTS iceberg.gold.cobertura_temporal")
        cur.execute("""
            CREATE TABLE iceberg.gold.cobertura_temporal AS
            SELECT batalhao_origem, subunidade, inicio_gap, fim_gap, gap_minutos
            FROM (
                SELECT batalhao_origem, subunidade,
                       lag(timestamp_geracao) OVER w AS inicio_gap,
                       timestamp_geracao AS fim_gap,
                       (to_unixtime(timestamp_geracao) -
                        to_unixtime(lag(timestamp_geracao) OVER w)) / 60.0 AS gap_minutos
                FROM iceberg.silver.gps
                WINDOW w AS (PARTITION BY batalhao_origem, subunidade ORDER BY timestamp_geracao)
            )
            WHERE inicio_gap IS NOT NULL
        """)
        print("Gold cobertura_temporal OK")
        trino_c.close()

    t_gold = PythonOperator(
        task_id="pipeline_gold_trino",
        python_callable=pipeline_gold_trino,
    )

    def sync_baseline_inline(**context):
        """TRUNCATE + reload do Silver (Trino) para PostgreSQL."""
        from psycopg2.extras import execute_values

        BATCH = 2000
        tabelas = [
            ("gps_posicionamento",
             "SELECT batalhao_origem, subunidade, latitude, longitude, altitude, velocidade, direcao, timestamp_geracao, timestamp_chegada, id_lote FROM iceberg.silver.gps",
             "INSERT INTO gps_posicionamento (batalhao_origem, subunidade, latitude, longitude, altitude, velocidade, direcao, timestamp_geracao, timestamp_chegada, id_lote) VALUES %s"),
            ("pessoal_subunidade",
             "SELECT id_relatorio, batalhao_origem, subunidade, situacao_operacional, efetivo_organico, efetivo_presente, baixas_combate, baixas_nao_combate, evacuados, necessidade_prioritaria, necessidade_logistica, timestamp_geracao, timestamp_chegada, id_lote FROM iceberg.silver.pessoal",
             "INSERT INTO pessoal_subunidade (id_relatorio, batalhao_origem, subunidade, situacao_operacional, efetivo_organico, efetivo_presente, baixas_combate, baixas_nao_combate, evacuados, necessidade_prioritaria, necessidade_logistica, timestamp_geracao, timestamp_chegada, id_lote) VALUES %s"),
            ("sensor_drone",
             "SELECT batalhao_origem, drone_id, area_cobertura, latitude_centro, longitude_centro, raio_km, altitude_voo, bateria_pct, status_missao, timestamp_geracao, timestamp_chegada, id_lote FROM iceberg.silver.sensor",
             "INSERT INTO sensor_drone (batalhao_origem, drone_id, area_cobertura, latitude_centro, longitude_centro, raio_km, altitude_voo, bateria_pct, status_missao, timestamp_geracao, timestamp_chegada, id_lote) VALUES %s"),
        ]

        trino_c = _get_trino_conn()
        pg_c = _get_pg_conn()

        for pg_table, trino_query, insert_sql in tabelas:
            trino_cur = trino_c.cursor()
            pg_cur = pg_c.cursor()

            pg_cur.execute(f"TRUNCATE TABLE {pg_table} RESTART IDENTITY")
            trino_cur.execute(trino_query)

            total = 0
            while True:
                rows = trino_cur.fetchmany(BATCH)
                if not rows:
                    break
                execute_values(pg_cur, insert_sql, rows)
                total += len(rows)

            pg_c.commit()
            print(f"  {pg_table}: {total} registros sincronizados")

        trino_c.close()
        pg_c.close()

    t_baseline = PythonOperator(
        task_id="pipeline_baseline_sync",
        python_callable=sync_baseline_inline,
    )

    t_criar_tabela = PythonOperator(
        task_id="criar_tabela_resultados",
        python_callable=criar_tabela_resultados,
    )

    t_contar = PythonOperator(
        task_id="contar_registros",
        python_callable=contar_registros,
    )

    t_benchmark = PythonOperator(
        task_id="executar_benchmark",
        python_callable=executar_benchmark,
    )

    # Dependências
    t_pausar >> t_gerar >> t_bronze >> t_silver >> t_gold >> t_baseline
    t_baseline >> [t_criar_tabela, t_contar]
    [t_criar_tabela, t_contar] >> t_benchmark
