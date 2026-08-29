"""
Injeta linhagem Bronze→Silver→Gold no OpenMetadata via REST API.

Uso:
    python3 inject_lineage.py
    python3 inject_lineage.py --om-url http://localhost:8585 --om-user admin --om-pass admin

Executar sempre que:
  - Os pipelines dag_silver_transform ou dag_gold_refresh forem rodados pela primeira vez
  - O OpenMetadata for resetado (docker compose down -v)
  - Novas tabelas Silver/Gold forem adicionadas
"""
import argparse
import base64
import json
import sys
import urllib.request
import urllib.error


def get_token(om_url: str, user: str, password: str) -> str:
    pass_b64 = base64.b64encode(password.encode()).decode()
    body = json.dumps({"email": user, "password": pass_b64}).encode()
    req = urllib.request.Request(
        f"{om_url}/api/v1/users/login",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["accessToken"]


def get_table_ids(om_url: str, token: str) -> dict:
    """Returns map of FQN suffix → full id, e.g. 'bronze.dados' → uuid"""
    headers = {"Authorization": f"Bearer {token}"}
    req = urllib.request.Request(
        f"{om_url}/api/v1/tables?database=trino_lakehouse.iceberg&limit=50&fields=id,fullyQualifiedName",
        headers=headers,
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    result = {}
    for t in data.get("data", []):
        fqn: str = t["fullyQualifiedName"]
        # key = last 2 parts: schema.table
        key = ".".join(fqn.split(".")[-2:])
        result[key] = t["id"]
    return result


def put_lineage(om_url: str, token: str, edge: dict) -> bool:
    body = json.dumps({"edge": edge}).encode()
    req = urllib.request.Request(
        f"{om_url}/api/v1/lineage",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as exc:
        print(f"  ERROR {exc.code}: {exc.read().decode()[:200]}", file=sys.stderr)
        return False


def col(service: str, schema: str, table: str, column: str) -> str:
    return f"trino_lakehouse.iceberg.{schema}.{table}.{column}"


def build_edges(ids: dict) -> list:
    B = "bronze.dados"
    edges = []

    # ── Bronze → Silver.GPS ─────────────────────────────────────────────────
    # from_col(payload) maps extracted fields; direct columns pass through
    edges.append({
        "fromEntity": {"id": ids[B], "type": "table"},
        "toEntity": {"id": ids["silver.gps"], "type": "table"},
        "lineageDetails": {
            "sqlQuery": "MERGE INTO lakehouse.silver.gps USING (SELECT from_json(payload,...) ...) ON id_registro WHEN NOT MATCHED THEN INSERT *",
            "columnsLineage": [
                {"toColumn": col("", "silver", "gps", "id_registro"),      "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "gps", "batalhao_origem"),   "fromColumns": [col("", "bronze", "dados", "batalhao_origem")]},
                {"toColumn": col("", "silver", "gps", "latitude"),          "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "gps", "longitude"),         "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "gps", "altitude"),          "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "gps", "velocidade"),        "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "gps", "direcao"),           "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "gps", "subunidade"),        "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "gps", "timestamp_geracao"), "fromColumns": [col("", "bronze", "dados", "timestamp_geracao")]},
                {"toColumn": col("", "silver", "gps", "timestamp_chegada"), "fromColumns": [col("", "bronze", "dados", "timestamp_chegada")]},
                {"toColumn": col("", "silver", "gps", "id_lote"),           "fromColumns": [col("", "bronze", "dados", "id_lote")]},
                {"toColumn": col("", "silver", "gps", "latencia_ingestao_s"), "fromColumns": [col("", "bronze", "dados", "timestamp_geracao"), col("", "bronze", "dados", "timestamp_chegada")]},
                {"toColumn": col("", "silver", "gps", "fora_de_ordem"),     "fromColumns": [col("", "bronze", "dados", "timestamp_geracao"), col("", "bronze", "dados", "timestamp_chegada")]},
            ],
        },
    })

    # ── Bronze → Silver.SITREP ───────────────────────────────────────────────
    edges.append({
        "fromEntity": {"id": ids[B], "type": "table"},
        "toEntity": {"id": ids["silver.sitrep"], "type": "table"},
        "lineageDetails": {
            "sqlQuery": "MERGE INTO lakehouse.silver.sitrep USING sitrep_novos ON id_registro WHEN NOT MATCHED THEN INSERT *",
            "columnsLineage": [
                {"toColumn": col("", "silver", "sitrep", "id_registro"),      "fromColumns": [col("", "bronze", "dados", "id_registro")]},
                {"toColumn": col("", "silver", "sitrep", "batalhao_origem"),   "fromColumns": [col("", "bronze", "dados", "batalhao_origem")]},
                {"toColumn": col("", "silver", "sitrep", "timestamp_geracao"), "fromColumns": [col("", "bronze", "dados", "timestamp_geracao")]},
                {"toColumn": col("", "silver", "sitrep", "timestamp_chegada"), "fromColumns": [col("", "bronze", "dados", "timestamp_chegada")]},
                {"toColumn": col("", "silver", "sitrep", "id_lote"),           "fromColumns": [col("", "bronze", "dados", "id_lote")]},
                {"toColumn": col("", "silver", "sitrep", "latencia_ingestao_s"), "fromColumns": [col("", "bronze", "dados", "timestamp_geracao"), col("", "bronze", "dados", "timestamp_chegada")]},
                {"toColumn": col("", "silver", "sitrep", "fora_de_ordem"),     "fromColumns": [col("", "bronze", "dados", "timestamp_geracao"), col("", "bronze", "dados", "timestamp_chegada")]},
            ],
        },
    })

    # ── Bronze → Silver.SENSOR ───────────────────────────────────────────────
    edges.append({
        "fromEntity": {"id": ids[B], "type": "table"},
        "toEntity": {"id": ids["silver.sensor"], "type": "table"},
        "lineageDetails": {
            "sqlQuery": "MERGE INTO lakehouse.silver.sensor USING sensor_novos ON id_registro WHEN NOT MATCHED THEN INSERT *",
            "columnsLineage": [
                {"toColumn": col("", "silver", "sensor", "id_registro"),       "fromColumns": [col("", "bronze", "dados", "payload")]},
                {"toColumn": col("", "silver", "sensor", "batalhao_origem"),    "fromColumns": [col("", "bronze", "dados", "batalhao_origem")]},
                {"toColumn": col("", "silver", "sensor", "timestamp_geracao"),  "fromColumns": [col("", "bronze", "dados", "timestamp_geracao")]},
                {"toColumn": col("", "silver", "sensor", "timestamp_chegada"),  "fromColumns": [col("", "bronze", "dados", "timestamp_chegada")]},
                {"toColumn": col("", "silver", "sensor", "id_lote"),            "fromColumns": [col("", "bronze", "dados", "id_lote")]},
                {"toColumn": col("", "silver", "sensor", "latencia_ingestao_s"), "fromColumns": [col("", "bronze", "dados", "timestamp_geracao"), col("", "bronze", "dados", "timestamp_chegada")]},
                {"toColumn": col("", "silver", "sensor", "fora_de_ordem"),      "fromColumns": [col("", "bronze", "dados", "timestamp_geracao"), col("", "bronze", "dados", "timestamp_chegada")]},
            ],
        },
    })

    # ── Silver.GPS → Gold.posicionamento_atual ───────────────────────────────
    GPS = "silver.gps"
    pass_through_gps = ["id_registro", "batalhao_origem", "subunidade", "latitude", "longitude",
                        "altitude", "velocidade", "direcao", "timestamp_geracao", "timestamp_chegada",
                        "id_lote", "latencia_ingestao_s", "fora_de_ordem", "processado_em"]
    edges.append({
        "fromEntity": {"id": ids[GPS], "type": "table"},
        "toEntity": {"id": ids["gold.posicionamento_atual"], "type": "table"},
        "lineageDetails": {
            "sqlQuery": "SELECT *, ROW_NUMBER() OVER (PARTITION BY batalhao_origem, subunidade ORDER BY timestamp_geracao DESC) rn FROM silver.gps WHERE rn=1",
            "columnsLineage": [
                {"toColumn": col("", "gold", "posicionamento_atual", c), "fromColumns": [col("", "silver", "gps", c)]}
                for c in pass_through_gps
            ],
        },
    })

    # ── Silver.SITREP → Gold.sitrep_consolidado ──────────────────────────────
    edges.append({
        "fromEntity": {"id": ids["silver.sitrep"], "type": "table"},
        "toEntity": {"id": ids["gold.sitrep_consolidado"], "type": "table"},
        "lineageDetails": {
            "sqlQuery": "SELECT batalhao_origem, date_trunc('hour', timestamp_geracao) janela_hora, COUNT(*) total_sitreps, ... FROM silver.sitrep GROUP BY 1,2",
            "columnsLineage": [
                {"toColumn": col("", "gold", "sitrep_consolidado", "batalhao_origem"),        "fromColumns": [col("", "silver", "sitrep", "batalhao_origem")]},
                {"toColumn": col("", "gold", "sitrep_consolidado", "janela_hora"),            "fromColumns": [col("", "silver", "sitrep", "timestamp_geracao")]},
                {"toColumn": col("", "gold", "sitrep_consolidado", "total_sitreps"),          "fromColumns": [col("", "silver", "sitrep", "id_registro")]},
                {"toColumn": col("", "gold", "sitrep_consolidado", "ultimo_sitrep"),          "fromColumns": [col("", "silver", "sitrep", "timestamp_geracao")]},
                {"toColumn": col("", "gold", "sitrep_consolidado", "total_baixas_proprias"),  "fromColumns": [col("", "silver", "sitrep", "baixas_proprias")]},
                {"toColumn": col("", "gold", "sitrep_consolidado", "total_baixas_inimigas"),  "fromColumns": [col("", "silver", "sitrep", "baixas_inimigas")]},
                {"toColumn": col("", "gold", "sitrep_consolidado", "situacoes"),              "fromColumns": [col("", "silver", "sitrep", "situacao")]},
            ],
        },
    })

    # ── Silver.GPS → Gold.latencia_por_batalhao ──────────────────────────────
    for src_schema, src_table in [("silver", "gps"), ("silver", "sitrep"), ("silver", "sensor")]:
        src_key = f"{src_schema}.{src_table}"
        edges.append({
            "fromEntity": {"id": ids[src_key], "type": "table"},
            "toEntity": {"id": ids["gold.latencia_por_batalhao"], "type": "table"},
            "lineageDetails": {
                "sqlQuery": "SELECT batalhao_origem, AVG(latencia_ingestao_s), APPROX_PERCENTILE(latencia_ingestao_s, 0.5), ... FROM silver.* GROUP BY batalhao_origem",
                "columnsLineage": [
                    {"toColumn": col("", "gold", "latencia_por_batalhao", "batalhao_origem"),   "fromColumns": [col("", src_schema, src_table, "batalhao_origem")]},
                    {"toColumn": col("", "gold", "latencia_por_batalhao", "latencia_media_s"),  "fromColumns": [col("", src_schema, src_table, "latencia_ingestao_s")]},
                    {"toColumn": col("", "gold", "latencia_por_batalhao", "p50_s"),             "fromColumns": [col("", src_schema, src_table, "latencia_ingestao_s")]},
                    {"toColumn": col("", "gold", "latencia_por_batalhao", "p90_s"),             "fromColumns": [col("", src_schema, src_table, "latencia_ingestao_s")]},
                    {"toColumn": col("", "gold", "latencia_por_batalhao", "p99_s"),             "fromColumns": [col("", src_schema, src_table, "latencia_ingestao_s")]},
                    {"toColumn": col("", "gold", "latencia_por_batalhao", "total_registros"),   "fromColumns": [col("", src_schema, src_table, "id_registro")]},
                ],
            },
        })

    # ── Silver.GPS → Gold.cobertura_temporal ─────────────────────────────────
    edges.append({
        "fromEntity": {"id": ids[GPS], "type": "table"},
        "toEntity": {"id": ids["gold.cobertura_temporal"], "type": "table"},
        "lineageDetails": {
            "sqlQuery": "SELECT batalhao_origem, subunidade, LAG(timestamp_geracao) inicio_gap, timestamp_geracao fim_gap, (timestamp_geracao - LAG(timestamp_geracao))/60 gap_minutos FROM silver.gps WHERE gap_minutos > 60",
            "columnsLineage": [
                {"toColumn": col("", "gold", "cobertura_temporal", "batalhao_origem"), "fromColumns": [col("", "silver", "gps", "batalhao_origem")]},
                {"toColumn": col("", "gold", "cobertura_temporal", "subunidade"),      "fromColumns": [col("", "silver", "gps", "subunidade")]},
                {"toColumn": col("", "gold", "cobertura_temporal", "inicio_gap"),      "fromColumns": [col("", "silver", "gps", "timestamp_geracao")]},
                {"toColumn": col("", "gold", "cobertura_temporal", "fim_gap"),         "fromColumns": [col("", "silver", "gps", "timestamp_geracao")]},
                {"toColumn": col("", "gold", "cobertura_temporal", "gap_minutos"),     "fromColumns": [col("", "silver", "gps", "timestamp_geracao")]},
            ],
        },
    })

    # ── Silver.SENSOR → Gold.atividade_sensores ──────────────────────────────
    edges.append({
        "fromEntity": {"id": ids["silver.sensor"], "type": "table"},
        "toEntity": {"id": ids["gold.atividade_sensores"], "type": "table"},
        "lineageDetails": {
            "sqlQuery": "SELECT batalhao_origem, area_cobertura, COUNT(*) missoes_totais, AVG(bateria_pct), MAX(timestamp_geracao), SUM(CASE WHEN status_missao='ativo' THEN 1 ELSE 0 END) FROM silver.sensor GROUP BY 1,2",
            "columnsLineage": [
                {"toColumn": col("", "gold", "atividade_sensores", "batalhao_origem"),    "fromColumns": [col("", "silver", "sensor", "batalhao_origem")]},
                {"toColumn": col("", "gold", "atividade_sensores", "area_cobertura"),     "fromColumns": [col("", "silver", "sensor", "area_cobertura")]},
                {"toColumn": col("", "gold", "atividade_sensores", "missoes_totais"),     "fromColumns": [col("", "silver", "sensor", "id_registro")]},
                {"toColumn": col("", "gold", "atividade_sensores", "bateria_media_pct"),  "fromColumns": [col("", "silver", "sensor", "bateria_pct")]},
                {"toColumn": col("", "gold", "atividade_sensores", "ultima_atividade"),   "fromColumns": [col("", "silver", "sensor", "timestamp_geracao")]},
                {"toColumn": col("", "gold", "atividade_sensores", "missoes_ativas"),     "fromColumns": [col("", "silver", "sensor", "status_missao")]},
            ],
        },
    })

    return edges


def main():
    parser = argparse.ArgumentParser(description="Inject Bronze→Silver→Gold lineage into OpenMetadata")
    parser.add_argument("--om-url", default="http://localhost:8585")
    parser.add_argument("--om-user", default="admin@open-metadata.org")
    parser.add_argument("--om-pass", default="admin")
    args = parser.parse_args()

    print(f"Connecting to OpenMetadata at {args.om_url} ...")
    token = get_token(args.om_url, args.om_user, args.om_pass)
    print("  Auth OK")

    print("Fetching table IDs ...")
    ids = get_table_ids(args.om_url, token)
    required = ["bronze.dados", "silver.gps", "silver.sitrep", "silver.sensor",
                "gold.posicionamento_atual", "gold.sitrep_consolidado",
                "gold.latencia_por_batalhao", "gold.cobertura_temporal", "gold.atividade_sensores"]
    missing = [k for k in required if k not in ids]
    if missing:
        print(f"ERROR: tables not found in OpenMetadata: {missing}", file=sys.stderr)
        print("  Run Trino metadata ingestion first (ingestao2 service).")
        sys.exit(1)
    print(f"  Found {len(ids)} tables")

    edges = build_edges(ids)
    print(f"Injecting {len(edges)} lineage edges ...")

    ok = err = 0
    for edge in edges:
        from_fqn = f"{edge['fromEntity']['type']}:{edge['fromEntity']['id']}"
        to_fqn = f"{edge['toEntity']['type']}:{edge['toEntity']['id']}"
        # Resolve FQN labels for display
        rev = {v: k for k, v in ids.items()}
        from_label = rev.get(edge['fromEntity']['id'], edge['fromEntity']['id'])
        to_label = rev.get(edge['toEntity']['id'], edge['toEntity']['id'])
        print(f"  {from_label} → {to_label} ... ", end="", flush=True)
        if put_lineage(args.om_url, token, edge):
            print("OK")
            ok += 1
        else:
            print("FAILED")
            err += 1

    print(f"\nDone: {ok} OK, {err} errors")
    if err:
        sys.exit(1)


if __name__ == "__main__":
    main()
