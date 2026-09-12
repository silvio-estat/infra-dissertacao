"""
DAG extracao_ocr — le os PDFs escaneados com reconhecimento optico e grava o
que leu em EXTRACAO. Aqui HA inferencia (INFERENCIA_INDIC = S).

    conferir_pendentes ──► extrair_ocr   (ha PDF em ARQUIVO sem linha em EXTRACAO)
                       └─► nada_a_fazer  (todo PDF ja foi lido)

Por que OCR: os PDFs deste estudo sao imagem de papel — o formulario foi
impresso, preenchido a mao, assinado e digitalizado. Nao ha camada de texto para
copiar; as letras precisam ser ADIVINHADAS a partir dos pixels. Por isso a linha
em EXTRACAO registra ferramenta, modelo e, principalmente, INFERENCIA_INDIC = S:
o resultado e estimativa e pode errar.

Cada PDF vira UMA linha em EXTRACAO, com o texto de todas as paginas. Achar o
que e "Ef Pres" dentro desse texto e trabalho do de/para, na Silver.

Nao usa Spark: o tesseract e um programa local que le uma imagem por vez.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from helpers.lineage_emitter import linhagem

IDIOMA = "por"          # o modelo de portugues do tesseract
RESOLUCAO = 200         # pontos por polegada ao rasterizar
SEGMENTACAO = 4         # como o tesseract divide a pagina: 4 = uma coluna de texto
                        # com tamanhos variados, que e o formato deste formulario

# Os dois numeros acima foram MEDIDOS, nao escolhidos. As 7 remessas que chegam
# em planilha E em escaneado dao o gabarito de graca, porque a planilha e a
# verdade por construcao. Contando quantos numeros da planilha aparecem no texto
# que o OCR devolveu, sobre os 276 numeros dos 7 pares:
#     200 dpi, psm 3 (padrao) ... 75,7%
#     200 dpi, psm 4 .......... 79,3%      <- adotado
#     300 dpi, qualquer psm ... 27%
# Rasterizar a 300 PIORA muito: o gerador digitalizou a 200, e ampliar borra o
# que nao existe. psm 4 nunca ficou abaixo de psm 3 em nenhum dos 7 arquivos.

PENDENTES_SQL = """
    SELECT a.arquivo_idt, a.arquivo_uri_txt
    FROM iceberg.bronze.arquivo a
    LEFT JOIN iceberg.bronze.extracao e ON e.arquivo_idt = a.arquivo_idt
    WHERE a.modalidade_cod = 'PDF' AND e.arquivo_idt IS NULL
    ORDER BY a.arquivo_uri_txt
"""

# Quando EXTRACAO ainda nao existe — primeira rodada, ou a tabela foi recriada —
# nada foi extraido e tudo esta pendente. Sem isso a DAG quebra com TABLE_NOT_FOUND
# numa tarefa de decisao, que e o pior lugar para uma surpresa.
TODOS_SQL = """
    SELECT a.arquivo_idt, a.arquivo_uri_txt
    FROM iceberg.bronze.arquivo a
    WHERE a.modalidade_cod = 'PDF'
    ORDER BY a.arquivo_uri_txt
"""


def conexao_trino():
    import trino
    return trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http")


def pdfs_pendentes() -> list:
    """[(arquivo_idt, uri)] do que ainda nao tem linha em EXTRACAO."""
    for sql in (PENDENTES_SQL, TODOS_SQL):
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


def ler_por_ocr(conteudo: bytes) -> dict:
    """Rasteriza cada pagina e devolve o texto que o tesseract reconheceu.

    Mesma forma de saida da leitura de planilha — um dicionario com o conteudo
    por unidade do documento —, para que o de/para nao precise saber por qual
    caminho o texto chegou.
    """
    import pdf2image
    import pytesseract

    paginas = pdf2image.convert_from_bytes(conteudo, dpi=RESOLUCAO)
    return {"paginas": [pytesseract.image_to_string(p, lang=IDIOMA, config=f"--psm {SEGMENTACAO}")
                        for p in paginas]}


def conferir_pendentes() -> str:
    pendentes = pdfs_pendentes()
    print(f"PDFs sem extracao: {len(pendentes)}")
    return "extrair_ocr" if pendentes else "nada_a_fazer"


def extrair_ocr():
    import pytesseract
    ferramenta = f"tesseract {pytesseract.get_tesseract_version()}"

    linhas = []
    for arquivo_idt, uri in pdfs_pendentes():
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            saida = json.dumps(ler_por_ocr(baixar(uri)), ensure_ascii=False)
            status = "OK"
        except Exception as erro:          # PDF corrompido ou pagina ilegivel
            saida, status = None, "FALHA"
            print(f"FALHA {uri}: {erro}")
        extracao_idt = "ext_" + hashlib.sha256(
            f"{arquivo_idt}|{ferramenta}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((extracao_idt, arquivo_idt, "S", ferramenta, IDIOMA,
                       f"psm{SEGMENTACAO}-{RESOLUCAO}dpi", saida,
                       round(time.perf_counter() - inicio, 3), status, agora))
        print(f"  {uri.rsplit('/', 1)[-1]}: {status} em {linhas[-1][7]}s")

    if not linhas:
        print("nada a extrair")
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
    print(f"EXTRACAO: {len(linhas)} linhas gravadas com {ferramenta} "
          f"({sum(1 for l in linhas if l[8] == 'FALHA')} falhas)")


with DAG(
    dag_id="extracao_ocr",
    description="ARQUIVO (PDF) -> EXTRACAO por reconhecimento optico, so o que falta",
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
        task_id="extrair_ocr",
        python_callable=extrair_ocr,
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
