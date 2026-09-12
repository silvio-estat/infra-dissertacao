"""
DAG extracao — tira o conteudo de dentro dos binarios e grava em EXTRACAO.

    conferir_pendentes ──► planilha ──► ocr ──► voz ──► texto
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
    texto     um modelo de linguagem le o que a VOZ escreveu e devolve campos

A ultima e a unica cuja entrada NAO e um binario: ela le a SAIDA de outra
extracao. Por isso grava EXTRACAO_ORIGEM_IDT — sem essa coluna as duas
inferencias ficariam penduradas no mesmo .wav como se fossem independentes, e se
perderia que uma leu a outra.

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
VOZ_MODELO, VOZ_IDIOMA = "medium", "pt"
VOZ_VOCABULARIO = "vocab-v1"

# --- modelo de linguagem. Roda FORA do Docker, no host: o Docker Desktop para
# Linux nao expoe a placa de video a um conteiner. Medido: 0,8 s por mensagem na
# GPU (RTX 5060 Ti) contra 18,2 s em CPU (i5 12400F) — a arquitetura nao exige
# GPU, so termina mais cedo com uma.
LLM_ENDERECO = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
LLM_MODELO = "qwen3.5:4b"
LLM_PROMPT_VERSAO = "voz-v1"


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
        linhas.append((idt, arquivo_idt, None, inferencia, ferramenta, modelo, ajuste, saida,
                       round(time.perf_counter() - inicio, 3), status, agora))
        if len(linhas) % 25 == 0:
            print(f"  {len(linhas)} lidos")

    gravar(linhas, modalidade, ferramenta)


COLUNAS = ("extracao_idt, arquivo_idt, extracao_origem_idt, inferencia_indic, ferramenta_nome, "
           "modelo_nome, prompt_versao_cod, saida_txt, extracao_segundos, status_cod, execucao_data")


def gravar(linhas, rotulo, ferramenta):
    """Uma insercao so: um snapshot Iceberg por rodada, nao um por arquivo."""
    if not linhas:
        print(f"nada a extrair em {rotulo}")
        return
    cur = conexao_trino().cursor()
    marcadores = ", ".join(["(" + ", ".join(["?"] * 11) + ")"] * len(linhas))
    cur.execute(f"INSERT INTO iceberg.bronze.extracao ({COLUNAS}) VALUES {marcadores}",
                [v for linha in linhas for v in linha])
    cur.fetchall()
    segundos = sum(l[8] for l in linhas)
    print(f"EXTRACAO: {len(linhas)} linhas de {rotulo} com {ferramenta}, "
          f"{segundos:.0f}s no total, {segundos/len(linhas):.1f}s cada "
          f"({sum(1 for l in linhas if l[9] == 'FALHA')} falhas)")


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

# =============================================================================
# A quarta: um modelo de linguagem le o que a transcricao escreveu
# =============================================================================

def transcricoes_sem_interpretacao() -> list:
    """[(extracao_idt, arquivo_idt, texto)] das transcricoes ainda nao interpretadas.

    A conta e pela propria coluna de cadeia: uma transcricao esta pendente
    enquanto nao existir outra extracao que a tenha lido.
    """
    sql = """
        SELECT t.extracao_idt, t.arquivo_idt, t.saida_txt
        FROM iceberg.bronze.extracao t
        JOIN iceberg.bronze.arquivo a ON a.arquivo_idt = t.arquivo_idt
        LEFT JOIN iceberg.bronze.extracao filha ON filha.extracao_origem_idt = t.extracao_idt
        WHERE a.modalidade_cod = 'AUDIO' AND t.status_cod = 'OK'
          AND filha.extracao_idt IS NULL
        ORDER BY t.extracao_idt
    """
    try:
        cur = conexao_trino().cursor()
        cur.execute(sql)
        return cur.fetchall()
    except Exception as erro:
        print(f"EXTRACAO nao consultada ({type(erro).__name__}); tratando como vazia")
        return []


def instrucao() -> str:
    """O prompt, montado a partir do modelo canonico e dos seeds.

    As listas sao as MESMAS que alimentam o vocabulario do transcritor. E a
    exigencia central e devolver o termo COMO ESTA NA LISTA: o que vem depois e
    consulta exata (`dominio` casa por sinonimo, `gazetteer` casa por nome), entao
    o trabalho do modelo nao e interpretar, e NORMALIZAR — transformar
    'posto mangavo' em 'Posto Mangaba'. Inventar um valor e pior que devolver
    nulo, porque um valor inventado nao casa e some em silencio.
    """
    modelo = yaml.safe_load(open(f"{CANONICO}/modelo_canonico.yaml", encoding="utf-8"))
    codinomes = sorted({s for v in modelo["dominios"]["tipo_evento"]["valores"].values()
                        for s in (v.get("sinonimos") or [])})
    with open(f"{CANONICO}/seeds/gazetteer.csv", encoding="utf-8") as fh:
        lugares = sorted({l["LOCAL_NOME"] for l in csv.DictReader(x for x in fh if not x.startswith("#"))})
    return (
        "Voce le a transcricao de uma mensagem de radio militar. A transcricao TEM ERROS: "
        "nomes proprios saem trocados por palavras parecidas. Reconheca, apesar do erro, qual "
        "termo das listas abaixo foi dito.\n\n"
        f"CODINOMES: {', '.join(codinomes)}\n\n"
        f"LUGARES: {', '.join(lugares)}\n\n"
        "Devolva so um JSON com tres chaves:\n"
        '  "codinome": um termo COPIADO da lista CODINOMES, ou null se nenhum foi dito\n'
        '  "referencia_local": um nome COPIADO da lista LUGARES, ou null\n'
        '  "texto": a mensagem sem o indicativo da estacao e sem as palavras de protocolo\n\n'
        "Regra rigida: codinome e referencia_local so podem conter texto que exista "
        "LITERALMENTE nas listas. Se o que foi dito nao estiver na lista, devolva null. "
        "Nunca escreva coordenadas, quadriculas ou nomes proprios que nao estejam listados.\n"
    )


def extrair_texto():
    """Cada transcricao vira uma linha NOVA em EXTRACAO, que aponta para ela."""
    import urllib.request

    prompt_base = instrucao()
    pendentes_ = transcricoes_sem_interpretacao()
    print(f"transcricoes sem interpretacao: {len(pendentes_)}")

    linhas = []
    for extracao_origem, arquivo_idt, saida_voz in pendentes_:
        texto = json.loads(saida_voz)["texto"]
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            corpo = json.dumps({"model": LLM_MODELO, "prompt": prompt_base + f"\nTRANSCRICAO: {texto}\n",
                                "stream": False, "format": "json", "think": False,
                                "options": {"temperature": 0}}).encode()
            req = urllib.request.Request(f"{LLM_ENDERECO}/api/generate", data=corpo,
                                         headers={"Content-Type": "application/json"})
            resposta = json.loads(urllib.request.urlopen(req, timeout=600).read())["response"]
            json.loads(resposta)        # so aceita o que e JSON de verdade
            saida, status = resposta, "OK"
        except Exception as erro:
            saida, status = None, "FALHA"
            print(f"FALHA {extracao_origem}: {erro}")
        idt = "ext_" + hashlib.sha256(
            f"{extracao_origem}|{LLM_MODELO}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((idt, arquivo_idt, extracao_origem, "S", "ollama", LLM_MODELO,
                       LLM_PROMPT_VERSAO, saida, round(time.perf_counter() - inicio, 3),
                       status, agora))
        if len(linhas) % 25 == 0:
            print(f"  {len(linhas)} interpretados")

    gravar(linhas, "TEXTO", f"ollama/{LLM_MODELO}")


def conferir_pendentes() -> str:
    quantos = {m: len(pendentes(m)) for m in ("PLANILHA", "PDF", "AUDIO")}
    quantos["transcricoes a interpretar"] = len(transcricoes_sem_interpretacao())
    print("pendente:", quantos)
    return "planilha" if sum(quantos.values()) else "nada_a_fazer"


def extrair_planilha():
    import openpyxl
    executar("PLANILHA", ler_planilha, f"openpyxl {openpyxl.__version__}", inferencia="N")


def extrair_ocr():
    import pytesseract
    executar("PDF", ler_ocr, f"tesseract {pytesseract.get_tesseract_version()}",
             inferencia="S", modelo=OCR_IDIOMA,
             ajuste=f"psm{OCR_SEGMENTACAO}-{OCR_RESOLUCAO}dpi")


def aceleracao() -> tuple:
    """(dispositivo, precisao) — usa a placa de video se houver uma visivel.

    A stack roda em CPU por padrao, porque e o hardware do ambiente de destino.
    Quando ha GPU o mesmo trabalho termina em uma fracao do tempo, sem mudar o
    resultado: e conveniencia de quem esta desenvolvendo, nao requisito.
    Em CPU a precisao e int8 (quantizada, para caber e andar); em GPU, float16.
    """
    try:
        import ctranslate2
        if ctranslate2.get_cuda_device_count() > 0:
            return "cuda", "float16"
    except Exception:
        pass
    return "cpu", "int8"


def extrair_voz():
    import faster_whisper

    dispositivo, precisao = aceleracao()
    print(f"transcrevendo em {dispositivo} ({precisao})")
    transcritor = faster_whisper.WhisperModel(VOZ_MODELO, device=dispositivo, compute_type=precisao)
    vocab = vocabulario()
    print(f"vocabulario inicial: {len(vocab)} caracteres")

    def ler(conteudo: bytes) -> dict:
        trechos, _ = transcritor.transcribe(io.BytesIO(conteudo), language=VOZ_IDIOMA,
                                            initial_prompt=vocab)
        return {"texto": " ".join(t.text.strip() for t in trechos).strip()}

    executar("AUDIO", ler, f"faster-whisper {faster_whisper.__version__}", inferencia="S",
             modelo=f"{VOZ_MODELO}-{precisao}", ajuste=VOZ_VOCABULARIO)


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
    texto = PythonOperator(
        task_id="texto",
        python_callable=extrair_texto,
        # a entrada e outra EXTRACAO, nao o arquivo: a linhagem e da tabela para ela mesma
        on_success_callback=linhagem(
            le=["bronze.extracao"], escreve=["bronze.extracao"],
            colunas={"saida_txt": [("bronze.extracao", "saida_txt")],
                     "extracao_origem_idt": [("bronze.extracao", "extracao_idt")]},
        ),
    )

    conferir >> [planilha, nada_a_fazer]
    planilha >> ocr >> voz >> texto
