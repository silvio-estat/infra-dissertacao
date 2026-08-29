"""
DAG: Governança Trino — OpenMetadata
Executa em sequência: ingestão de metadados → profiler → sample data.
Disparo manual.
"""
from __future__ import annotations

import base64
import os
import time
from datetime import datetime, timedelta

import requests
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

OM_API = "http://openmetadata:8585/api/v1"
TRINO_URL = "http://trino:8090"
SAMPLE_LIMIT = 50


# ---------------------------------------------------------------------------
# Task 1 — Ingestão de metadados
# ---------------------------------------------------------------------------
def run_metadata_ingestion():
    from metadata.workflow.metadata import MetadataWorkflow

    jwt = os.environ.get("OM_INGESTION_BOT_JWT", "")
    if not jwt:
        raise ValueError("OM_INGESTION_BOT_JWT não definido")

    config = f"""
source:
  type: trino
  serviceName: trino_lakehouse
  sourceConfig:
    config:
      type: DatabaseMetadata
      markDeletedTables: true
      overrideMetadata: true
processor:
  type: orm-metadata
  config: {{}}
sink:
  type: metadata-rest
  config: {{}}
workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "{jwt}"
"""
    wf = MetadataWorkflow.create(yaml.safe_load(config))
    wf.execute()
    wf.raise_from_status()
    wf.print_status()
    wf.stop()


# ---------------------------------------------------------------------------
# Task 2 — Profiler (exclui information_schema e bronze — sem dados analíticos)
# ---------------------------------------------------------------------------
def run_profiler():
    from metadata.workflow.profiler import ProfilerWorkflow

    jwt = os.environ.get("OM_INGESTION_BOT_JWT", "")
    if not jwt:
        raise ValueError("OM_INGESTION_BOT_JWT não definido")

    config = f"""
source:
  type: trino
  serviceName: trino_lakehouse
  serviceConnection:
    config:
      type: Trino
      hostPort: trino:8090
      username: admin
      catalog: iceberg
      databaseSchema: ""
      connectionArguments:
        http_scheme: http
  sourceConfig:
    config:
      type: Profiler
      threadCount: 2
      schemaFilterPattern:
        excludes:
          - information_schema
          - bronze
processor:
  type: orm-profiler
  config: {{}}
sink:
  type: metadata-rest
  config: {{}}
workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "{jwt}"
"""
    wf = ProfilerWorkflow.create(yaml.safe_load(config))
    wf.execute()
    wf.raise_from_status()
    wf.print_status()
    wf.stop()


# ---------------------------------------------------------------------------
# Task 3 — Data Quality (executa test suites cadastrados nas tabelas)
# ---------------------------------------------------------------------------
def _listar_tabelas_com_testes() -> list[str]:
    """Consulta a API do OM e retorna FQNs de tabelas do serviço trino_lakehouse que possuem test suites."""
    token = _login_om()
    headers = {"Authorization": f"Bearer {token}"}
    fqns = []
    offset = 0
    limit = 50

    while True:
        resp = requests.get(
            f"{OM_API}/dataQuality/testSuites",
            headers=headers,
            params={"limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        suites = data.get("data", [])

        for suite in suites:
            fqn = suite.get("basicEntityReference", {}).get("fullyQualifiedName", "")
            if fqn.startswith("trino_lakehouse."):
                fqns.append(fqn)

        if len(suites) < limit:
            break
        offset += limit

    return fqns


def run_data_quality():
    from metadata.workflow.data_quality import TestSuiteWorkflow

    jwt = os.environ.get("OM_INGESTION_BOT_JWT", "")
    if not jwt:
        raise ValueError("OM_INGESTION_BOT_JWT não definido")

    tabelas_com_testes = _listar_tabelas_com_testes()

    if not tabelas_com_testes:
        print("Nenhuma tabela com test suite encontrada no OM para trino_lakehouse.")
        return

    print(f"\nTabelas com testes encontradas ({len(tabelas_com_testes)}): {tabelas_com_testes}")

    for fqn in tabelas_com_testes:
        print(f"\nExecutando testes para: {fqn}")
        config = f"""
source:
  type: TestSuite
  serviceName: {fqn}
  sourceConfig:
    config:
      type: TestSuite
      entityFullyQualifiedName: "{fqn}"
processor:
  type: orm-test-runner
  config: {{}}
sink:
  type: metadata-rest
  config: {{}}
workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "{jwt}"
"""
        wf = TestSuiteWorkflow.create(yaml.safe_load(config))
        wf.execute()
        wf.print_status()
        wf.stop()


# ---------------------------------------------------------------------------
# Task 4 — Sample data
# ---------------------------------------------------------------------------
def _login_om():
    senha_b64 = base64.b64encode(b"admin").decode()
    email = os.environ.get("OM_ADMIN_EMAIL", "admin@open-metadata.org")
    resp = requests.post(
        f"{OM_API}/users/login",
        json={"email": email, "password": senha_b64},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["accessToken"]


def _listar_tabelas(token):
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
        and "information_schema" not in t.get("fullyQualifiedName", "")
    ]


def _fqn_para_trino(fqn):
    partes = fqn.split(".")
    if len(partes) < 4:
        return None
    return ".".join(partes[1:])


def _executar_query_trino(sql):
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
    linhas_str = [[str(v) if v is not None else "" for v in linha] for linha in linhas]
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
            print(f"  ERRO query Trino: {e}")
            erros.append(fqn)
            continue

        if not linhas:
            print(f"  Tabela vazia — sem sample data")
            continue

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
            print(f"  ERRO envio OM: {e}")
            erros.append(fqn)

    _limpar_constraints_null(token, tabelas)

    print(f"\n=== Resumo: {ok_count}/{len(tabelas)} tabelas com sample data ===")
    if erros:
        print("Erros:")
        for e in erros:
            print(f"  - {e}")

    return {"ok": ok_count, "total": len(tabelas), "erros": erros}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "dlh",
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_trino_governance",
    default_args=default_args,
    description="Governança Trino: metadados → profiler → sample data (sequencial)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["governance", "openmetadata", "trino"],
    is_paused_upon_creation=True,
) as dag:

    t_metadata = PythonOperator(
        task_id="ingestao_metadados",
        python_callable=run_metadata_ingestion,
        execution_timeout=timedelta(minutes=30),
    )

    t_profiler = PythonOperator(
        task_id="profiler",
        python_callable=run_profiler,
        execution_timeout=timedelta(minutes=30),
    )

    t_quality = PythonOperator(
        task_id="data_quality",
        python_callable=run_data_quality,
        execution_timeout=timedelta(minutes=30),
    )

    t_sample = PythonOperator(
        task_id="sample_data",
        python_callable=coletar_e_enviar,
        execution_timeout=timedelta(minutes=15),
    )

    t_metadata >> t_profiler >> t_quality >> t_sample
