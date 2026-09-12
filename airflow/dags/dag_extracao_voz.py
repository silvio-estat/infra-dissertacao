"""
DAG extracao_voz — transcreve as mensagens de radio e grava o texto em EXTRACAO.
Ha inferencia (INFERENCIA_INDIC = S).

    conferir_pendentes ──► transcrever   (ha AUDIO em ARQUIVO sem linha em EXTRACAO)
                       └─► nada_a_fazer  (todo audio ja foi transcrito)

O VOCABULARIO INICIAL e o detalhe que decide a qualidade. Os codinomes da
operacao sao palavras comuns usadas com sentido incomum ('cavalo', 'tordilho',
'orvalho') e os toponimos sao inventados; sem avisar o transcritor de que elas
existem, ele escreve o que soa parecido — 'todilho', 'ovalo'. A lista vem das
MESMAS fontes que o pipeline usa: os sinonimos do dominio de tipo de evento e os
nomes do gazetteer. Ela e versionada em PROMPT_VERSAO_COD, porque trocar a lista
muda o resultado.

CPU: o faster-whisper roda sobre CTranslate2 quantizado em int8, sem GPU.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone

import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from helpers.lineage_emitter import linhagem

MODELO = "medium"      # small dava WER 29,4%; medium e ~3x mais lento e erra menos
PRECISAO = "int8"
IDIOMA = "pt"
VERSAO_VOCABULARIO = "vocab-v1"
CANONICO = "/opt/canonico"          # o modelo e os seeds, montados no container

PENDENTES_SQL = """
    SELECT a.arquivo_idt, a.arquivo_uri_txt
    FROM iceberg.bronze.arquivo a
    LEFT JOIN iceberg.bronze.extracao e ON e.arquivo_idt = a.arquivo_idt
    WHERE a.modalidade_cod = 'AUDIO' AND e.arquivo_idt IS NULL
    ORDER BY a.arquivo_uri_txt
"""
TODOS_SQL = """
    SELECT a.arquivo_idt, a.arquivo_uri_txt
    FROM iceberg.bronze.arquivo a
    WHERE a.modalidade_cod = 'AUDIO'
    ORDER BY a.arquivo_uri_txt
"""


def conexao_trino():
    import trino
    return trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http")


def audios_pendentes() -> list:
    for sql in (PENDENTES_SQL, TODOS_SQL):
        try:
            cur = conexao_trino().cursor()
            cur.execute(sql)
            return cur.fetchall()
        except Exception as erro:
            print(f"EXTRACAO nao consultada ({type(erro).__name__}); tratando como vazia")
    return []


def vocabulario() -> str:
    """As palavras raras que o transcritor precisa saber que existem.

    Sai do modelo canonico e dos seeds — nao de uma lista escrita a mao —, entao
    acrescentar um codinome ou um lugar ao estudo ja melhora a transcricao.
    """
    modelo = yaml.safe_load(open(f"{CANONICO}/modelo_canonico.yaml", encoding="utf-8"))
    codinomes = [s for v in modelo["dominios"]["tipo_evento"]["valores"].values()
                 for s in (v.get("sinonimos") or [])]
    with open(f"{CANONICO}/seeds/gazetteer.csv", encoding="utf-8") as fh:
        lugares = [l["LOCAL_NOME"] for l in csv.DictReader(x for x in fh if not x.startswith("#"))]
    return "Rede de radio militar. Termos: " + ", ".join(sorted(set(codinomes + lugares))) + "."


def baixar(uri: str) -> bytes:
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


def conferir_pendentes() -> str:
    pendentes = audios_pendentes()
    print(f"audios sem transcricao: {len(pendentes)}")
    return "transcrever" if pendentes else "nada_a_fazer"


def transcrever():
    import io
    import faster_whisper

    modelo = faster_whisper.WhisperModel(MODELO, device="cpu", compute_type=PRECISAO)
    ferramenta = f"faster-whisper {faster_whisper.__version__}"
    vocab = vocabulario()
    print(f"vocabulario inicial: {len(vocab)} caracteres")

    linhas = []
    for arquivo_idt, uri in audios_pendentes():
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            trechos, _ = modelo.transcribe(io.BytesIO(baixar(uri)), language=IDIOMA,
                                           initial_prompt=vocab)
            texto = " ".join(t.text.strip() for t in trechos).strip()
            saida, status = json.dumps({"texto": texto}, ensure_ascii=False), "OK"
        except Exception as erro:
            saida, status = None, "FALHA"
            print(f"FALHA {uri}: {erro}")
        extracao_idt = "ext_" + hashlib.sha256(
            f"{arquivo_idt}|{ferramenta}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((extracao_idt, arquivo_idt, "S", ferramenta, f"{MODELO}-{PRECISAO}",
                       VERSAO_VOCABULARIO, saida,
                       round(time.perf_counter() - inicio, 3), status, agora))
        if len(linhas) % 25 == 0:
            print(f"  {len(linhas)} transcritos")

    if not linhas:
        print("nada a transcrever")
        return
    cur = conexao_trino().cursor()
    marcadores = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(linhas))
    cur.execute(
        f"INSERT INTO iceberg.bronze.extracao (extracao_idt, arquivo_idt, inferencia_indic, "
        f"ferramenta_nome, modelo_nome, prompt_versao_cod, saida_txt, extracao_segundos, "
        f"status_cod, execucao_data) VALUES {marcadores}",
        [v for linha in linhas for v in linha],
    )
    cur.fetchall()
    segundos = sum(l[7] for l in linhas)
    print(f"EXTRACAO: {len(linhas)} linhas com {ferramenta} ({MODELO}/{PRECISAO}), "
          f"{segundos:.0f}s no total, {segundos/len(linhas):.1f}s por audio "
          f"({sum(1 for l in linhas if l[8] == 'FALHA')} falhas)")


with DAG(
    dag_id="extracao_voz",
    description="ARQUIVO (AUDIO) -> EXTRACAO por transcricao de fala, so o que falta",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dlh", "retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "extracao", "ia", "canonico"],
) as dag:

    conferir = BranchPythonOperator(task_id="conferir_pendentes", python_callable=conferir_pendentes)
    nada_a_fazer = EmptyOperator(task_id="nada_a_fazer")
    extrair = PythonOperator(
        task_id="transcrever",
        python_callable=transcrever,
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
