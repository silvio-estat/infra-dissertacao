"""
DAG extracao — tira o conteudo de dentro dos binarios e grava em EXTRACAO.

    conferir_pendentes ──► planilha ──► ocr ──► voz
                       └─► nada_a_fazer

Uma etapa do fluxo, uma DAG: ingestao ──► EXTRACAO ──► canonico_silver ──► gold.

Uma TAREFA por modalidade, e nao uma so para tudo, porque as ferramentas e os
tempos sao muito diferentes — 0,01 s por planilha, 0,7 s por PDF, 5,6 s por
audio — e uma falha no reconhecimento optico nao pode derrubar a transcricao
junto. Correm em serie: todas disputam a mesma CPU.

O que separa as tres e so COMO o conteudo e lido:

    planilha  openpyxl abre o .xlsx e copia as celulas      leitura direta, sem IA
    ocr       tesseract adivinha as letras dos pixels       inferencia
    voz       faster-whisper adivinha as palavras do som    inferencia

Tudo o mais — achar o que falta, baixar do MinIO, gravar — e o mesmo, e por isso
esta escrito uma vez so.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from helpers.lineage_emitter import linhagem

CANONICO = "/opt/canonico"      # o modelo e os seeds, montados no container

# --- ajustes do reconhecimento optico, MEDIDOS contra as 7 remessas que chegam
# em planilha e em escaneado (a planilha e a verdade por construcao):
#     200 dpi, psm 3 (padrao) 75,7% · 200 dpi, psm 4 79,3% · 300 dpi 27%
# Rasterizar a 300 piora: o gerador digitalizou a 200, e ampliar borra.
OCR_IDIOMA, OCR_RESOLUCAO, OCR_SEGMENTACAO = "por", 200, 4

# --- transcricao. medium contra small, sobre os mesmos 150 audios:
#     WER 21,6% contra 29,4% · codinome 93% contra 65% · 5,6 s contra 2,2 s
# O codinome vira TIPO_COD, entao o ganho compensa o triplo do tempo.
VOZ_MODELO, VOZ_PRECISAO, VOZ_IDIOMA = "medium", "int8", "pt"
VOZ_VOCABULARIO = "vocab-v1"


# =============================================================================
# O que e igual para as tres
# =============================================================================

def conexao_trino():
    import trino
    return trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http")


def pendentes(modalidade: str) -> list:
    """[(arquivo_idt, uri)] dos binarios desta modalidade que ainda nao foram lidos.

    Se EXTRACAO nao existe — primeira rodada, ou a tabela foi recriada — nada foi
    lido e tudo esta pendente. Sem isso a DAG quebra com TABLE_NOT_FOUND numa
    tarefa de decisao, que e o pior lugar para uma surpresa.
    """
    com_extracao = f"""
        SELECT a.arquivo_idt, a.arquivo_uri_txt
        FROM iceberg.bronze.arquivo a
        LEFT JOIN iceberg.bronze.extracao e ON e.arquivo_idt = a.arquivo_idt
        WHERE a.modalidade_cod = '{modalidade}' AND e.arquivo_idt IS NULL
        ORDER BY a.arquivo_uri_txt
    """
    sem_extracao = f"""
        SELECT a.arquivo_idt, a.arquivo_uri_txt
        FROM iceberg.bronze.arquivo a
        WHERE a.modalidade_cod = '{modalidade}'
        ORDER BY a.arquivo_uri_txt
    """
    for sql in (com_extracao, sem_extracao):
        try:
            cur = conexao_trino().cursor()
            cur.execute(sql)
            return cur.fetchall()
        except Exception as erro:
            print(f"EXTRACAO nao consultada ({type(erro).__name__}); tratando como vazia")
    return []


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


def executar(modalidade, ler, ferramenta, inferencia, modelo=None, ajuste=None):
    """Le todos os binarios pendentes de uma modalidade e grava o que saiu.

    `ler` e a unica coisa que muda entre as tres tarefas: recebe os bytes e
    devolve um dicionario com o conteudo. O resto — achar o que falta, baixar,
    cronometrar, registrar a falha sem parar a fila, gravar numa insercao so —
    e identico, e por isso mora aqui.
    """
    linhas = []
    for arquivo_idt, uri in pendentes(modalidade):
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            saida, status = json.dumps(ler(baixar(uri)), ensure_ascii=False), "OK"
        except Exception as erro:      # arquivo corrompido: fica registrado, a fila segue
            saida, status = None, "FALHA"
            print(f"FALHA {uri}: {erro}")
        idt = "ext_" + hashlib.sha256(
            f"{arquivo_idt}|{ferramenta}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((idt, arquivo_idt, inferencia, ferramenta, modelo, ajuste, saida,
                       round(time.perf_counter() - inicio, 3), status, agora))
        if len(linhas) % 25 == 0:
            print(f"  {len(linhas)} lidos")

    if not linhas:
        print(f"nada a extrair em {modalidade}")
        return
    # uma insercao so: um snapshot Iceberg por rodada, nao um por arquivo
    cur = conexao_trino().cursor()
    marcadores = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(linhas))
    cur.execute(
        "INSERT INTO iceberg.bronze.extracao (extracao_idt, arquivo_idt, inferencia_indic, "
        "ferramenta_nome, modelo_nome, prompt_versao_cod, saida_txt, extracao_segundos, "
        f"status_cod, execucao_data) VALUES {marcadores}",
        [v for linha in linhas for v in linha],
    )
    cur.fetchall()
    segundos = sum(l[7] for l in linhas)
    print(f"EXTRACAO: {len(linhas)} linhas de {modalidade} com {ferramenta}, "
          f"{segundos:.0f}s no total, {segundos/len(linhas):.1f}s por arquivo "
          f"({sum(1 for l in linhas if l[8] == 'FALHA')} falhas)")


# =============================================================================
# O que muda: como cada modalidade e lida
# =============================================================================

def ler_planilha(conteudo: bytes) -> dict:
    """Todas as celulas, aba por aba, linha por linha. Datas viram texto ISO.
    Deterministico: mesma planilha, mesma saida, sempre."""
    import openpyxl

    def valor(c):
        return c.isoformat() if isinstance(c, (datetime, date)) else c

    wb = openpyxl.load_workbook(io.BytesIO(conteudo), read_only=True, data_only=True)
    return {"abas": {aba.title: [[valor(c) for c in linha] for linha in aba.iter_rows(values_only=True)]
                     for aba in wb.worksheets}}


def ler_ocr(conteudo: bytes) -> dict:
    """Rasteriza cada pagina e devolve o texto que o tesseract reconheceu.
    Os PDFs deste estudo sao imagem de papel: nao ha camada de texto para copiar,
    as letras precisam ser adivinhadas dos pixels."""
    import pdf2image
    import pytesseract

    paginas = pdf2image.convert_from_bytes(conteudo, dpi=OCR_RESOLUCAO)
    return {"paginas": [pytesseract.image_to_string(p, lang=OCR_IDIOMA,
                                                    config=f"--psm {OCR_SEGMENTACAO}")
                        for p in paginas]}


def vocabulario() -> str:
    """As palavras raras que o transcritor precisa saber que existem.

    Os codinomes sao palavras comuns com sentido incomum ('cavalo', 'tordilho') e
    os toponimos sao inventados. A lista sai do MODELO CANONICO e dos seeds, nao
    de um texto escrito a mao: acrescentar um codinome ao estudo ja melhora a
    transcricao.
    """
    modelo = yaml.safe_load(open(f"{CANONICO}/modelo_canonico.yaml", encoding="utf-8"))
    codinomes = [s for v in modelo["dominios"]["tipo_evento"]["valores"].values()
                 for s in (v.get("sinonimos") or [])]
    with open(f"{CANONICO}/seeds/gazetteer.csv", encoding="utf-8") as fh:
        lugares = [l["LOCAL_NOME"] for l in csv.DictReader(x for x in fh if not x.startswith("#"))]
    return "Rede de radio militar. Termos: " + ", ".join(sorted(set(codinomes + lugares))) + "."


# =============================================================================
# As tarefas
# =============================================================================

def conferir_pendentes() -> str:
    quantos = {m: len(pendentes(m)) for m in ("PLANILHA", "PDF", "AUDIO")}
    print("binarios sem extracao:", quantos)
    return "planilha" if sum(quantos.values()) else "nada_a_fazer"


def extrair_planilha():
    import openpyxl
    executar("PLANILHA", ler_planilha, f"openpyxl {openpyxl.__version__}", inferencia="N")


def extrair_ocr():
    import pytesseract
    executar("PDF", ler_ocr, f"tesseract {pytesseract.get_tesseract_version()}",
             inferencia="S", modelo=OCR_IDIOMA,
             ajuste=f"psm{OCR_SEGMENTACAO}-{OCR_RESOLUCAO}dpi")


def extrair_voz():
    import faster_whisper

    transcritor = faster_whisper.WhisperModel(VOZ_MODELO, device="cpu", compute_type=VOZ_PRECISAO)
    vocab = vocabulario()
    print(f"vocabulario inicial: {len(vocab)} caracteres")

    def ler(conteudo: bytes) -> dict:
        trechos, _ = transcritor.transcribe(io.BytesIO(conteudo), language=VOZ_IDIOMA,
                                            initial_prompt=vocab)
        return {"texto": " ".join(t.text.strip() for t in trechos).strip()}

    executar("AUDIO", ler, f"faster-whisper {faster_whisper.__version__}", inferencia="S",
             modelo=f"{VOZ_MODELO}-{VOZ_PRECISAO}", ajuste=VOZ_VOCABULARIO)


def tarefa(nome, funcao) -> PythonOperator:
    """Toda tarefa le ARQUIVO e escreve EXTRACAO — a linhagem e a mesma."""
    return PythonOperator(
        task_id=nome,
        python_callable=funcao,
        on_success_callback=linhagem(
            le=["bronze.arquivo"],
            escreve=["bronze.extracao"],
            colunas={
                "arquivo_idt": [("bronze.arquivo", "arquivo_idt")],
                "saida_txt":   [("bronze.arquivo", "arquivo_uri_txt")],
            },
        ),
    )


with DAG(
    dag_id="2_bronze_extracao",
    description="ARQUIVO -> EXTRACAO: planilha por leitura direta, PDF por OCR, audio por transcricao",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dlh", "retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["bronze", "extracao", "canonico"],
) as dag:

    conferir = BranchPythonOperator(task_id="conferir_pendentes", python_callable=conferir_pendentes)
    nada_a_fazer = EmptyOperator(task_id="nada_a_fazer")

    planilha = tarefa("planilha", extrair_planilha)
    ocr = tarefa("ocr", extrair_ocr)
    voz = tarefa("voz", extrair_voz)

    conferir >> [planilha, nada_a_fazer]
    planilha >> ocr >> voz
