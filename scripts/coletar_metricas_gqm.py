"""
Coleta dos Indicadores GQM — ARQ_TEMAC
Estudo de caso: arquitetura Lakehouse (Iceberg + Trino) para C2.

Não há comparação com outro paradigma. Os indicadores CARACTERIZAM a arquitetura:
preservação de registros, latências, throughput, desempenho analítico, footprint
e evolução de schema.

Execução (fora do Docker, na máquina host):
  python3 scripts/coletar_metricas_gqm.py

Pré-requisitos:
  pip install trino psycopg2-binary boto3 tabulate
"""

import time
import statistics
import json
from datetime import datetime

import trino
import psycopg2
import boto3
from tabulate import tabulate

# ── Conexões ─────────────────────────────────────────────────────────────────

TRINO_CONF = dict(host="localhost", port=8090, user="admin", http_scheme="http")
MINIO_CONF = dict(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio_admin",
    aws_secret_access_key="minio_pass_2026",
)
# airflow_db é infraestrutura (orquestração), consultado apenas para medir throughput
AIRFLOW_DB_CONF = dict(host="localhost", port=5432, database="airflow_db",
                       user="dlh_admin", password="dlh_pass_2026")

resultados = {}


def trino_conn():
    return trino.dbapi.connect(**TRINO_CONF)


def executar_trino(sql):
    con = trino_conn()
    cur = con.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    con.close()
    return rows


def medir_query_trino(sql, repeticoes=5):
    """Executa a query N vezes e retorna (media_s, desvio_s, tempos)."""
    tempos = []
    for _ in range(repeticoes):
        t0 = time.perf_counter()
        executar_trino(sql)
        tempos.append(time.perf_counter() - t0)
    return statistics.mean(tempos), statistics.stdev(tempos) if len(tempos) > 1 else 0, tempos


# ── Indicador 1 — Registros Preservados ──────────────────────────────────────

def indicador_1():
    print("\n=== I1 — Registros Preservados (Consistência do Pipeline) ===")

    bronze        = executar_trino("SELECT count(*) FROM iceberg.bronze.dados")[0][0]
    silver_gps    = executar_trino("SELECT count(*) FROM iceberg.silver.gps")[0][0]
    silver_pessoal= executar_trino("SELECT count(*) FROM iceberg.silver.pessoal")[0][0]
    silver_sensor = executar_trino("SELECT count(*) FROM iceberg.silver.sensor")[0][0]
    gold_coc      = executar_trino("SELECT count(*) FROM iceberg.gold.coc")[0][0]
    gold_lat      = executar_trino("SELECT count(*) FROM iceberg.gold.latencia_por_batalhao")[0][0]

    # Tipos distintos no Bronze para calcular preservação por tipo
    bronze_gps    = executar_trino("SELECT count(*) FROM iceberg.bronze.dados WHERE tipo_dado = 'gps'")[0][0]
    bronze_pessoal= executar_trino("SELECT count(*) FROM iceberg.bronze.dados WHERE tipo_dado = 'pessoal'")[0][0]
    bronze_sensor = executar_trino("SELECT count(*) FROM iceberg.bronze.dados WHERE tipo_dado = 'sensor'")[0][0]

    taxa_gps     = silver_gps     / bronze_gps     * 100 if bronze_gps     else 0
    taxa_pessoal = silver_pessoal / bronze_pessoal * 100 if bronze_pessoal else 0
    taxa_sensor  = silver_sensor  / bronze_sensor  * 100 if bronze_sensor  else 0

    dados = [
        ["Bronze total",        bronze,         "—",           "—"],
        ["Bronze GPS",          bronze_gps,     "—",           "—"],
        ["Bronze Pessoal",      bronze_pessoal, "—",           "—"],
        ["Bronze Sensor",       bronze_sensor,  "—",           "—"],
        ["Silver GPS",          silver_gps,     bronze_gps,    f"{taxa_gps:.2f}%"],
        ["Silver Pessoal",      silver_pessoal, bronze_pessoal,f"{taxa_pessoal:.2f}%"],
        ["Silver Sensor",       silver_sensor,  bronze_sensor, f"{taxa_sensor:.2f}%"],
        ["Gold COC",            gold_coc,       "—",           "—"],
        ["Gold latencia",       gold_lat,       "—",           "—"],
    ]
    print(tabulate(dados, headers=["Camada", "Registros", "Origem Bronze", "Taxa Preservação"]))

    resultados["I1"] = {
        "bronze_total": bronze,
        "silver_gps": silver_gps, "silver_pessoal": silver_pessoal, "silver_sensor": silver_sensor,
        "taxa_preservacao_gps_pct": taxa_gps,
        "taxa_preservacao_pessoal_pct": taxa_pessoal,
        "taxa_preservacao_sensor_pct": taxa_sensor,
    }


# ── Indicador 2 — Latência de Ingestão ───────────────────────────────────────

def indicador_2():
    print("\n=== I2 — Latência de Ingestão (Bronze: timestamp_geracao → timestamp_chegada) ===")

    rows = executar_trino("""
        SELECT tipo_dado, media_s, min_s, max_s, p50_s, p90_s, p99_s FROM (
            SELECT 'gps' AS tipo_dado,
                AVG(latencia_ingestao_s) AS media_s, MIN(latencia_ingestao_s) AS min_s,
                MAX(latencia_ingestao_s) AS max_s,
                APPROX_PERCENTILE(latencia_ingestao_s, 0.5)  AS p50_s,
                APPROX_PERCENTILE(latencia_ingestao_s, 0.9)  AS p90_s,
                APPROX_PERCENTILE(latencia_ingestao_s, 0.99) AS p99_s
            FROM iceberg.silver.gps WHERE latencia_ingestao_s IS NOT NULL
            UNION ALL
            SELECT 'pessoal',
                AVG(latencia_ingestao_s), MIN(latencia_ingestao_s), MAX(latencia_ingestao_s),
                APPROX_PERCENTILE(latencia_ingestao_s, 0.5),
                APPROX_PERCENTILE(latencia_ingestao_s, 0.9),
                APPROX_PERCENTILE(latencia_ingestao_s, 0.99)
            FROM iceberg.silver.pessoal WHERE latencia_ingestao_s IS NOT NULL
            UNION ALL
            SELECT 'sensor',
                AVG(latencia_ingestao_s), MIN(latencia_ingestao_s), MAX(latencia_ingestao_s),
                APPROX_PERCENTILE(latencia_ingestao_s, 0.5),
                APPROX_PERCENTILE(latencia_ingestao_s, 0.9),
                APPROX_PERCENTILE(latencia_ingestao_s, 0.99)
            FROM iceberg.silver.sensor WHERE latencia_ingestao_s IS NOT NULL
        ) ORDER BY tipo_dado
    """)

    print(tabulate(rows, headers=["Tipo", "Média(s)", "Min(s)", "Max(s)", "p50(s)", "p90(s)", "p99(s)"],
                   floatfmt=".4f"))

    resultados["I2"] = {"por_tipo": [list(r) for r in rows]}


# ── Indicador 3 — Latência Ponta-a-Ponta ─────────────────────────────────────

def indicador_3():
    print("\n=== I3 — Latência Ponta-a-Ponta por Batalhão (geracao → Silver processado_em) ===")

    rows = executar_trino("""
        SELECT
            batalhao_origem,
            AVG(EXTRACT(SECOND FROM (processado_em - timestamp_geracao)) +
                EXTRACT(MINUTE FROM (processado_em - timestamp_geracao)) * 60 +
                EXTRACT(HOUR  FROM (processado_em - timestamp_geracao)) * 3600) AS latencia_media_s,
            MIN(EXTRACT(SECOND FROM (processado_em - timestamp_geracao)) +
                EXTRACT(MINUTE FROM (processado_em - timestamp_geracao)) * 60 +
                EXTRACT(HOUR  FROM (processado_em - timestamp_geracao)) * 3600) AS latencia_min_s,
            MAX(EXTRACT(SECOND FROM (processado_em - timestamp_geracao)) +
                EXTRACT(MINUTE FROM (processado_em - timestamp_geracao)) * 60 +
                EXTRACT(HOUR  FROM (processado_em - timestamp_geracao)) * 3600) AS latencia_max_s,
            count(*) AS registros
        FROM iceberg.silver.gps
        GROUP BY batalhao_origem
        ORDER BY batalhao_origem
    """)

    print(tabulate(rows, headers=["Batalhão", "Média(s)", "Min(s)", "Max(s)", "Registros"],
                   floatfmt=".2f"))

    resultados["I3"] = {"por_batalhao": [list(r) for r in rows]}


# ── Indicador 4 — Throughput das DAGs ────────────────────────────────────────

def indicador_4():
    """Lê o banco do Airflow (infraestrutura) para medir tempo de execução das DAGs."""
    print("\n=== I4 — Throughput: tempos de execução das DAGs ===")

    try:
        con = psycopg2.connect(**AIRFLOW_DB_CONF)
        cur = con.cursor()
        cur.execute("""
            SELECT
                dag_id,
                ROUND(EXTRACT(EPOCH FROM (end_date - start_date))::numeric, 2) AS duracao_s
            FROM dag_run
            WHERE dag_id IN ('dag_ingestao_bronze','dag_silver_transform','dag_gold_refresh')
              AND state = 'success'
            ORDER BY dag_id, end_date DESC
            LIMIT 12
        """)
        rows = cur.fetchall()
        con.close()

        # Volumes de referência por DAG (ajustar conforme a rodada)
        dag_totais = {
            "dag_ingestao_bronze":  46020,
            "dag_silver_transform": 7920 + 15520 + 7920,
            "dag_gold_refresh":     7 + 10 + 7 + 0 + 7,
        }

        tabela = []
        seen = {}
        for dag_id, dur_s in rows:
            if dag_id not in seen:
                seen[dag_id] = float(dur_s)

        for dag_id, dur_s in seen.items():
            total = dag_totais.get(dag_id, "?")
            throughput = f"{total / dur_s:.1f}" if isinstance(total, int) and dur_s > 0 else "?"
            tabela.append([dag_id, total, f"{dur_s:.1f}s", throughput])

        print(tabulate(tabela, headers=["DAG", "Registros", "Duração", "Throughput (rec/s)"]))
        resultados["I4"] = {"dag_throughput": tabela}

    except Exception as e:
        print(f"  Erro ao ler Airflow DB: {e}")
        resultados["I4"] = {"erro": str(e)}


# ── Indicador 5 — Desempenho das Consultas Analíticas ────────────────────────

QUERIES_ANALITICAS = {
    "Q1_ultima_posicao": """
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
    "Q3_fusao_multifonte_1h": """
        WITH janela AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem,
                   AVG(latitude)  AS lat_media,
                   AVG(longitude) AS lon_media,
                   count(*)       AS registros_gps
            FROM iceberg.silver.gps
            GROUP BY 1, 2
        ),
        sit AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem,
                   count(*)                                     AS registros_pessoal,
                   SUM(baixas_combate + baixas_nao_combate)    AS baixas
            FROM iceberg.silver.pessoal
            GROUP BY 1, 2
        ),
        sen AS (
            SELECT date_trunc('hour', timestamp_geracao) AS hora,
                   batalhao_origem,
                   count(*) AS registros_sensor
            FROM iceberg.silver.sensor
            GROUP BY 1, 2
        )
        SELECT j.batalhao_origem, j.hora,
               j.registros_gps, j.lat_media, j.lon_media,
               COALESCE(s.registros_pessoal, 0) AS registros_pessoal,
               COALESCE(s.baixas, 0)            AS baixas,
               COALESCE(d.registros_sensor, 0)  AS registros_sensor
        FROM janela j
        LEFT JOIN sit s ON j.batalhao_origem = s.batalhao_origem AND j.hora = s.hora
        LEFT JOIN sen d ON j.batalhao_origem = d.batalhao_origem AND j.hora = d.hora
        ORDER BY j.batalhao_origem, j.hora
    """,
    "Q4_latencia_percentis": """
        SELECT batalhao_origem,
               latencia_media_s AS media_s,
               p50_s,
               p90_s,
               p99_s
        FROM iceberg.gold.latencia_por_batalhao
        ORDER BY batalhao_origem
    """,
    "Q5_gaps_cobertura": """
        SELECT batalhao_origem, subunidade,
               inicio_gap, fim_gap,
               gap_minutos
        FROM iceberg.gold.cobertura_temporal
        ORDER BY gap_minutos DESC
    """,
}


def indicador_5():
    print("\n=== I5 — Desempenho das Consultas Analíticas (5 execuções cada) ===")

    tabela = []
    i5_dados = {}

    for nome, sql in QUERIES_ANALITICAS.items():
        print(f"  Executando {nome}...", end=" ", flush=True)
        media, desvio, tempos = medir_query_trino(sql)

        tabela.append([nome, f"{media:.3f}s", f"±{desvio:.3f}",
                       f"{min(tempos):.3f}s", f"{max(tempos):.3f}s"])
        i5_dados[nome] = {"media_s": media, "desvio_s": desvio,
                          "min_s": min(tempos), "max_s": max(tempos)}
        print("OK")

    print()
    print(tabulate(tabela, headers=["Query", "Média", "±σ", "Min", "Max"]))
    resultados["I5"] = i5_dados


# ── Indicador 6 — Footprint de Armazenamento ─────────────────────────────────

def indicador_6():
    print("\n=== I6 — Footprint de Armazenamento (MinIO) ===")

    s3 = boto3.client("s3", **MINIO_CONF)

    def prefix_size_mb(bucket, prefix=""):
        total = 0
        try:
            paginator = s3.get_paginator("list_objects_v2")
            kwargs = {"Bucket": bucket}
            if prefix:
                kwargs["Prefix"] = prefix
            for page in paginator.paginate(**kwargs):
                for obj in page.get("Contents", []):
                    total += obj["Size"]
        except Exception:
            pass
        return total / (1024 * 1024)

    landing_mb   = prefix_size_mb("landing")
    bronze_mb    = prefix_size_mb("lakehouse", "warehouse/bronze.db/")
    silver_mb    = prefix_size_mb("lakehouse", "warehouse/silver.db/")
    gold_mb      = prefix_size_mb("lakehouse", "warehouse/gold.db/")
    warehouse_mb = silver_mb + gold_mb

    taxa_landing_bronze   = landing_mb / bronze_mb    if bronze_mb    > 0 else 0
    taxa_bronze_warehouse = bronze_mb / warehouse_mb  if warehouse_mb > 0 else 0

    dados = [
        ["Landing (JSON bruto)",              f"{landing_mb:.2f} MB"],
        ["Bronze (lakehouse/bronze.db)",      f"{bronze_mb:.2f} MB"],
        ["Silver (lakehouse/silver.db)",      f"{silver_mb:.2f} MB"],
        ["Gold (lakehouse/gold.db)",          f"{gold_mb:.2f} MB"],
        ["Silver + Gold (Warehouse)",         f"{warehouse_mb:.2f} MB"],
        ["Razão Landing → Bronze (Parquet)",  f"{taxa_landing_bronze:.2f}x"],
        ["Razão Bronze → Warehouse",          f"{taxa_bronze_warehouse:.2f}x"],
    ]
    print(tabulate(dados, headers=["Item", "Valor"]))

    resultados["I6"] = {
        "landing_mb": landing_mb, "bronze_mb": bronze_mb,
        "silver_mb": silver_mb, "gold_mb": gold_mb,
        "warehouse_mb": warehouse_mb,
        "razao_landing_bronze": taxa_landing_bronze,
        "razao_bronze_warehouse": taxa_bronze_warehouse,
    }


# ── Indicador 7 — Schema Evolution ───────────────────────────────────────────

def indicador_7():
    print("\n=== I7 — Schema Evolution e Time Travel ===")

    rows = executar_trino("""
        SELECT snapshot_id, committed_at, summary
        FROM iceberg.silver."gps$snapshots"
        ORDER BY committed_at DESC
        LIMIT 5
    """)

    print(f"  Snapshots recentes da tabela silver.gps: {len(rows)}")
    for r in rows:
        print(f"    snapshot={r[0]}  committed={r[1]}")

    print("""
  Evolução de schema na arquitetura:
    Bronze  — JSON no campo payload (schema-on-read): campos novos chegam sem DDL.
    Silver  — ALTER TABLE ADD COLUMN idempotente em migrar_schema(); leituras antigas
              continuam válidas (schema evolution do Iceberg, sem reescrita de dados).
    Snapshots— cada commit gera um snapshot consultável por FOR TIMESTAMP AS OF,
              permitindo reconstruir o estado da tabela em qualquer instante.
    """)

    resultados["I7"] = {
        "snapshots_silver_gps": len(rows),
        "bronze": "schema-on-read (payload JSON) — campos novos não exigem DDL",
        "silver": "ALTER TABLE ADD COLUMN sem reescrita; snapshots preservam histórico",
    }


# ── Relatório Final ───────────────────────────────────────────────────────────

def salvar_relatorio():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"relatorios/metricas_gqm_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"coletado_em": ts, "indicadores": resultados}, f,
                  ensure_ascii=False, indent=2, default=str)
    print(f"\n✓ Resultados salvos em {path}")
    return path


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 70)
    print("  COLETA DE MÉTRICAS GQM — ARQ_TEMAC")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    indicador_1()
    indicador_2()
    indicador_3()
    indicador_4()
    indicador_5()
    indicador_6()
    indicador_7()

    salvar_relatorio()

    print("\n" + "=" * 70)
    print("  COLETA CONCLUÍDA")
    print("=" * 70)
