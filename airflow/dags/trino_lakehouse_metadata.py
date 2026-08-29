# Copyright 2025 Collate
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://www.apache.org/licenses/LICENSE-2.0
"""
DAG que executa a ingestão de METADADOS do serviço Trino (catalogo lakehouse).
O DAG tem o id **trino_lakehouse_metadata**, que é exatamente o nome que a UI
do OpenMetadata procura quando você abre *Database Services → trino_lakehouse →
trino_lakehouse_metadata*.
"""

import os
from datetime import datetime, timedelta
import yaml
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    from airflow.operators.python_operator import PythonOperator

from metadata.workflow.metadata import MetadataWorkflow

default_args = {
    "owner": "openmetadata",
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def run_metadata_ingestion():
    """Executa a ingestão de metadados do Trino via JWT do ingestion-bot."""
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
    workflow_cfg = yaml.safe_load(config)
    wf = MetadataWorkflow.create(workflow_cfg)
    wf.execute()
    wf.raise_from_status()
    wf.print_status()
    wf.stop()


with DAG(
    "trino_lakehouse_metadata",
    default_args=default_args,
    description="Ingestão de metadados do serviço Trino (lakehouse) para OpenMetadata",
    schedule_interval=None,  # disparado manualmente ou via webhook/evento
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    ingest = PythonOperator(
        task_id="run_trino_metadata_ingestion",
        python_callable=run_metadata_ingestion,
    )
