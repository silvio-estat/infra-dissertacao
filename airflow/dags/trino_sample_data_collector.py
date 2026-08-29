"""
DAG que coleta sample data das tabelas Trino (Iceberg) e envia para o OpenMetadata.

Usa API REST diretamente — sem dependência do SDK openmetadata-ingestion.
Conectividade: Airflow → Trino (trino:8090) e Airflow → OM (openmetadata:8585)
"""

from datetime import datetime, timedelta
import requests
import base64
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

OM_API = "http://openmetadata:8585/api/v1"
TRINO_URL = "http://trino:8090"
SAMPLE_LIMIT = 50


def _login_om():
    """Autentica no OpenMetadata como admin e retorna access token."""
    senha_b64 = base64.b64encode(b"admin").decode()
    resp = requests.post(
        f"{OM_API}/users/login",
        json={"email": "admin@open-metadata.org", "password": senha_b64},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["accessToken"]


def _listar_tabelas(token):
    """Retorna tabelas do serviço trino_lakehouse com nomes de colunas."""
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(
        f"{OM_API}/tables",
        headers=headers,
        params={"limit": 100, "fields": "columns"},
        timeout=30,
    )
    resp.raise_for_status()
    return [
        t
        for t in resp.json().get("data", [])
        if t.get("service", {}).get("name") == "trino_lakehouse"
    ]


def _fqn_para_trino(fqn):
    """Converte FQN do OM para nome qualificado no Trino.

    Exemplo: trino_lakehouse.iceberg.bronze.dados → iceberg.bronze.dados
    """
    partes = fqn.split(".")
    if len(partes) < 4:
        return None
    return ".".join(partes[1:])


def _executar_query_trino(sql):
    """Executa query no Trino via REST API e retorna (colunas, linhas)."""
    resp = requests.post(
        f"{TRINO_URL}/v1/statement",
        data=sql,
        headers={"X-Trino-User": "admin"},
        timeout=60,
    )
    resp.raise_for_status()

    colunas = []
    linhas = []
    resultado = resp.json()

    for _ in range(60):
        if "columns" in resultado and not colunas:
            colunas = [c["name"] for c in resultado["columns"]]
        if "data" in resultado:
            linhas.extend(resultado["data"])

        estado = resultado.get("stats", {}).get("state", "")
        next_uri = resultado.get("nextUri")

        if estado in ("FAILED", "CANCELED"):
            erro = resultado.get("error", {}).get("message", estado)
            raise RuntimeError(f"Query Trino falhou: {erro}")

        if not next_uri:
            break

        time.sleep(0.5)
        resp2 = requests.get(next_uri, headers={"X-Trino-User": "admin"}, timeout=30)
        resp2.raise_for_status()
        resultado = resp2.json()

    return colunas, linhas


def _enviar_sample_data(token, tabela_id, colunas, linhas):
    """Envia sample data para o OM via PUT /tables/{id}/sampleData."""
    linhas_str = [
        [str(v) if v is not None else "" for v in linha] for linha in linhas
    ]
    payload = {"columns": colunas, "rows": linhas_str}
    resp = requests.put(
        f"{OM_API}/tables/{tabela_id}/sampleData",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()


def _limpar_constraints_null(token, tabelas):
    """Remove constraint='NULL' das colunas — o conector Trino seta esse valor
    e o OM exibe 'NULL' ao lado de cada coluna na view de lineage."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json-patch+json",
    }
    for tabela in tabelas:
        patches = [
            {"op": "remove", "path": f"/columns/{i}/constraint"}
            for i, c in enumerate(tabela.get("columns", []))
            if c.get("constraint") == "NULL"
        ]
        if not patches:
            continue
        resp = requests.patch(
            f"{OM_API}/tables/{tabela['id']}",
            headers=headers,
            json=patches,
            timeout=10,
        )
        nome = tabela["fullyQualifiedName"].split(".", 2)[-1]
        if resp.status_code == 200:
            print(f"  Constraints limpos: {nome} ({len(patches)} colunas)")
        else:
            print(f"  AVISO constraint {nome}: HTTP {resp.status_code}")


def coletar_e_enviar(**context):
    """Task principal: coleta sample data de todas as tabelas Trino e envia ao OM."""
    print("=== Coletando sample data para OpenMetadata ===\n")

    token = _login_om()
    print("Login OM OK")

    tabelas = _listar_tabelas(token)
    print(f"Encontradas {len(tabelas)} tabelas no servico trino_lakehouse\n")

    ok_count = 0
    erros = []

    for tabela in tabelas:
        fqn = tabela["fullyQualifiedName"]
        tabela_id = tabela["id"]
        colunas_om = [c["name"] for c in tabela.get("columns", [])]
        nome_trino = _fqn_para_trino(fqn)

        if not nome_trino:
            print(f"  SKIP {fqn} — FQN invalido")
            continue

        print(f"Processando: {fqn}")
        sql = f"SELECT * FROM {nome_trino} LIMIT {SAMPLE_LIMIT}"

        try:
            colunas_trino, linhas = _executar_query_trino(sql)
        except Exception as e:
            msg = f"  ERRO query Trino: {e}"
            print(msg)
            erros.append(fqn)
            continue

        if not linhas:
            print(f"  Tabela vazia — sem sample data")
            continue

        # Reordenar colunas do Trino para coincidir com a ordem do OM
        if set(colunas_om) != set(colunas_trino):
            print(f"  AVISO: colunas OM={colunas_om} vs Trino={colunas_trino}")

        idx_map = {nome: i for i, nome in enumerate(colunas_trino)}
        colunas_final = [c for c in colunas_om if c in idx_map]
        indices = [idx_map[c] for c in colunas_final]
        linhas_reord = [[linha[i] for i in indices] for linha in linhas]

        try:
            _enviar_sample_data(token, tabela_id, colunas_final, linhas_reord)
            print(f"  OK — {len(colunas_final)} colunas, {len(linhas_reord)} linhas")
            ok_count += 1
        except Exception as e:
            msg = f"  ERRO envio OM: {e}"
            print(msg)
            erros.append(fqn)

    _limpar_constraints_null(token, tabelas)

    print(f"\n=== Resumo: {ok_count}/{len(tabelas)} tabelas com sample data ===")
    if erros:
        print("Erros:")
        for e in erros:
            print(f"  - {e}")

    return {"ok": ok_count, "total": len(tabelas), "erros": erros}


default_args = {
    "owner": "dlh",
    "retries": 1,
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    "trino_sample_data_collector",
    default_args=default_args,
    description="Coleta sample data das tabelas Trino e envia para OpenMetadata",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    PythonOperator(
        task_id="coletar_sample_data",
        python_callable=coletar_e_enviar,
    )
