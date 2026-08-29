# Copyright 2025 Collate
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://www.apache.org/licenses/LICENSE-2.0
"""
DAG que executa o profiler do Trino para coletar estatísticas de Data Observability
e enviar ao OpenMetadata (aba "Profiler & Data Quality").

O profiler conecta diretamente ao Trino via Iceberg catalog e coleta:
- rowCount, columnCount, nullCount por coluna
- min, max, média, desvio padrão, distintos

Uso:
  docker exec dlh_airflow_webserver airflow dags trigger trino_lakehouse_profiler
"""

from datetime import datetime, timedelta
import yaml
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    from airflow.operators.python_operator import PythonOperator

import os

default_args = {
    "owner": "openmetadata",
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def run_profiler():
    """Executa o profiler do Trino para coletar estatísticas de colunas.

    Usa JWT do ingestion-bot via OM_INGESTION_BOT_JWT.
    authProvider: openmetadata com securityConfig.jwtToken é o modo correto
    para Airflow externo ao container OM.
    """
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
      threadCount: 1
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

    workflow_cfg = yaml.safe_load(config)
    wf = ProfilerWorkflow.create(workflow_cfg)
    wf.execute()
    wf.raise_from_status()
    wf.print_status()
    wf.stop()


with DAG(
    "trino_lakehouse_profiler",
    default_args=default_args,
    description="Profiler do Trino (Iceberg) - coleta sample data e estatísticas para OpenMetadata",
    schedule_interval=None,  # disparado manualmente
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    profiler = PythonOperator(
        task_id="run_trino_profiler",
        python_callable=run_profiler,
    )