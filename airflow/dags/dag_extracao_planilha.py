"""
DAG extracao_planilha — le as planilhas catalogadas em ARQUIVO e grava o que
leu em EXTRACAO. Leitura direta, sem IA (INFERENCIA_INDIC = N).

    conferir_pendentes ──► extrair_planilhas   (ha PLANILHA em ARQUIVO sem linha em EXTRACAO)
                       └─► nada_a_fazer        (toda planilha ja foi lida)

Cada planilha vira UMA linha em EXTRACAO: SAIDA_TXT guarda todas as celulas,
aba por aba, linha por linha, como texto JSON — nenhuma interpretacao. Achar
o cabecalho e ler "Ef Pres" da linha certa e trabalho do de/para (transformacao
`celula`), na Silver. A ferramenta e sua versao ficam registradas: uma
releitura com outra versao acrescenta uma linha nova, nunca altera a anterior.

Nao usa Spark: abrir 98 planilhas nao e trabalho distribuido. E o mesmo
formato que as extracoes por IA (OCR, transcricao) seguirao.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from helpers.lineage_emitter import linhagem

TABELA_ARQUIVO = "iceberg.bronze.arquivo"
TABELA_EXTRACAO = "iceberg.bronze.extracao"
PENDENTES_SQL = f"""
    SELECT a.arquivo_idt, a.arquivo_uri_txt
    FROM {TABELA_ARQUIVO} a
    LEFT JOIN {TABELA_EXTRACAO} e ON e.arquivo_idt = a.arquivo_idt
    WHERE a.modalidade_cod = 'PLANILHA' AND e.arquivo_idt IS NULL
    ORDER BY a.arquivo_uri_txt
"""


def conexao_trino():
    import trino
    return trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http")


def planilhas_pendentes() -> list:
    """[(arquivo_idt, uri)] das planilhas que ainda nao tem linha em EXTRACAO."""
    cur = conexao_trino().cursor()
    cur.execute(PENDENTES_SQL)
    return cur.fetchall()


def baixar(uri: str) -> bytes:
    """s3a://lakehouse/landing/... -> bytes, direto do MinIO."""
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
    )
    bucket, chave = uri.replace("s3a://", "").split("/", 1)
    return s3.get_object(Bucket=bucket, Key=chave)["Body"].read()


def ler_celulas(conteudo: bytes) -> dict:
    """Todas as celulas, aba por aba, linha por linha. Datas viram texto ISO."""
    import openpyxl

    def valor(c):
        return c.isoformat() if isinstance(c, (datetime, date)) else c

    wb = openpyxl.load_workbook(io.BytesIO(conteudo), read_only=True, data_only=True)
    return {"abas": {aba.title: [[valor(c) for c in linha] for linha in aba.iter_rows(values_only=True)]
                     for aba in wb.worksheets}}


def conferir_pendentes() -> str:
    pendentes = planilhas_pendentes()
    print(f"planilhas sem extracao: {len(pendentes)}")
    return "extrair_planilhas" if pendentes else "nada_a_fazer"


def extrair_planilhas():
    import openpyxl
    ferramenta = f"openpyxl {openpyxl.__version__}"

    linhas = []
    for arquivo_idt, uri in planilhas_pendentes():
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            saida = json.dumps(ler_celulas(baixar(uri)), ensure_ascii=False)
            status = "OK"
        except Exception as erro:             # planilha corrompida, nao abre: fica registrado
            saida, status = None, "FALHA"
            print(f"FALHA {uri}: {erro}")
        extracao_idt = "ext_" + hashlib.sha256(f"{arquivo_idt}|{ferramenta}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((extracao_idt, arquivo_idt, "N", ferramenta, None, None, saida,
                       round(time.perf_counter() - inicio, 3), status, agora))

    if not linhas:
        print("nada a extrair")
        return
    # uma insercao so: um snapshot Iceberg, nao um por planilha
    cur = conexao_trino().cursor()
    marcadores = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(linhas))
    cur.execute(
        f"INSERT INTO {TABELA_EXTRACAO} (extracao_idt, arquivo_idt, inferencia_indic, ferramenta_nome, "
        f"modelo_nome, prompt_versao_cod, saida_txt, extracao_segundos, status_cod, execucao_data) "
        f"VALUES {marcadores}",
        [v for linha in linhas for v in linha],
    )
    cur.fetchall()
    print(f"EXTRACAO: {len(linhas)} linhas gravadas com {ferramenta} "
          f"({sum(1 for l in linhas if l[8] == 'FALHA')} falhas)")


with DAG(
    dag_id="extracao_planilha",
    description="ARQUIVO (PLANILHA) -> EXTRACAO por leitura direta com openpyxl, so o que falta",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dlh", "retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "extracao", "canonico"],
) as dag:

    conferir = BranchPythonOperator(task_id="conferir_pendentes", python_callable=conferir_pendentes)
    nada_a_fazer = EmptyOperator(task_id="nada_a_fazer")
    extrair = PythonOperator(
        task_id="extrair_planilhas",
        python_callable=extrair_planilhas,
        on_success_callback=linhagem(
            le=["bronze.arquivo"],
            escreve=["bronze.extracao"],
            colunas={
                "arquivo_idt": [("bronze.arquivo", "arquivo_idt")],
                "saida_txt":   [("bronze.arquivo", "arquivo_uri_txt")],
            },
        ),
    )

    conferir >> [extrair, nada_a_fazer]
