"""
DAG extracao — tira o conteudo de dentro dos binarios e grava em EXTRACAO.

    conferir_pendentes ──► planilha ──► ocr ──► tabela ──► voz ──► texto ──► relato ──► informe
                       └─► nada_a_fazer

Uma etapa do fluxo, uma DAG: ingestao ──► EXTRACAO ──► canonico_silver ──► gold.

Uma TAREFA por modalidade, e nao uma so para tudo, porque as ferramentas e os
tempos sao muito diferentes — 0,01 s por planilha, 0,7 s por PDF, 5,6 s por
audio — e uma falha no reconhecimento optico nao pode derrubar a transcricao
junto. Correm em serie: todas disputam a mesma CPU.

O que separa as tres e so COMO o conteudo e lido:

    planilha  openpyxl abre o .xlsx e copia as celulas      leitura direta, sem IA
    ocr       tesseract adivinha as letras dos pixels       inferencia
    tabela    docling reconstroi a grade do PDF escaneado   inferencia
    voz       faster-whisper adivinha as palavras do som    inferencia
    texto     um modelo de linguagem le o que a VOZ escreveu e devolve campos
    informe   um modelo de linguagem le o que o OCR leu do INTEL e devolve campos

A `texto` e a `informe` nao leem um binario: leem a SAIDA de outra
extracao. Por isso grava EXTRACAO_ORIGEM_IDT — sem essa coluna as duas
inferencias ficariam penduradas no mesmo .wav como se fossem independentes, e se
perderia que uma leu a outra.

Tudo o mais — achar o que falta, baixar do MinIO, gravar — e o mesmo, e por isso
esta escrito uma vez so.

Toda tarefa com ajuste (todas menos a planilha) confere antes, em AJUSTE_EXTRACAO,
o prompt montado e os parametros com que vai chamar a ferramenta: versao nova e
registrada, a mesma segue, e o mesmo codigo com texto diferente PARA a tarefa.
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
LLM_PROMPT_VERSAO = "voz-v2"
# as palavras-codigo de prioridade faladas no fim da mensagem de radio. Estao entre
# os sinonimos do dominio `prioridade` do modelo canonico, misturadas com as palavras
# dos formularios ("Alta", "Normal"); aqui so as do radio, para o modelo nao ler
# "deslocamento normal" como prioridade.
PRIORIDADES_RADIO = ["chuva", "orvalho", "relampago"]
LLM_PROMPT_VERSAO_RELATO = "relato-v3"
LLM_PROMPT_VERSAO_INTEL = "intel-v1"
# as opcoes de TODA chamada ao modelo; as mesmas vao para AJUSTE_EXTRACAO
LLM_OPCOES = {"format": "json", "think": False, "options": {"temperature": 0}}

# --- tabela de PDF escaneado. O Docling reconstroi a GRADE (linha e coluna de cada
# celula), o que o tesseract nao faz. Medido: RELPER 95,7% dos numeros na celula
# certa (7 pares planilha x escaneado); FOGOS 11/11 areas bombardeadas na coluna
# certa (ACHADOS.md, 13/09). Roda como servico, como o Ollama.
DOCLING_URL = os.environ.get("DOCLING_URL", "http://docling:5001")
DOCLING_AJUSTE = "docling-rapidocr-accurate-v1"
# as opcoes com que o acerto foi medido: no OCR, `auto` pode escolher outro motor
DOCLING_OPCOES = {"to_formats": "json", "do_ocr": "true", "force_ocr": "true",
                  "do_table_structure": "true", "table_mode": "accurate",
                  "ocr_preset": "rapidocr", "images_scale": "1.0", "include_images": "false"}

# --- a cada quantos itens TODA tarefa grava. Uma queda do computador perde no
# maximo um lote, e a rodada seguinte recomeca de onde parou: toda tarefa pergunta
# antes o que ainda nao foi lido. Em 12/09 um reinicio no meio perdeu 250 relatos,
# porque a gravacao era uma so, no fim.
LOTE = 50


# =============================================================================
# O que e igual para as tres
# =============================================================================

def conexao_trino():
    import trino
    return trino.dbapi.connect(host="trino", port=8090, user="airflow", http_scheme="http")


def consultar(*consultas: str) -> list:
    """Roda a primeira consulta que der certo e devolve as linhas.

    O UNICO erro tolerado e tabela inexistente — primeira rodada, ou a tabela foi
    recriada: passa para a consulta seguinte (a mesma pergunta sem EXTRACAO) e, se
    nao sobrar nenhuma, a resposta e vazia.

    Qualquer outro erro sobe e a tarefa fica VERMELHA. Em 13/09, com o Trino ainda
    subindo depois de um reinicio, "nao consegui perguntar" foi lido como "nao ha
    nada pendente" e a tarefa marcou sucesso sem ter feito nada.
    """
    from trino.exceptions import TrinoQueryError

    for sql in consultas:
        try:
            cur = conexao_trino().cursor()
            cur.execute(sql)
            return cur.fetchall()
        except TrinoQueryError as erro:
            if erro.error_name != "TABLE_NOT_FOUND":
                raise
            print(f"tabela ausente: {erro.message}")
    return []


def pendentes(modalidade: str) -> list:
    """[(arquivo_idt, uri)] dos binarios desta modalidade que ainda nao foram lidos.

    Se EXTRACAO nao existe, nada foi lido e tudo esta pendente.
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
    return consultar(com_extracao, sem_extracao)


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


def executar(modalidade, ler, ferramenta, inferencia, modelo=None, ajuste=None,
             parametros=None, prompt=None, fila=None):
    """Le todos os binarios pendentes de uma modalidade e grava o que saiu.

    `ler` e a unica coisa que muda entre as tres tarefas: recebe os bytes e
    devolve um dicionario com o conteudo. O resto — achar o que falta, baixar,
    cronometrar, registrar a falha sem parar a fila, gravar a cada LOTE —
    e identico, e por isso mora aqui.

    `ajuste`, `parametros` e `prompt` dizem COMO a ferramenta e chamada; sao
    conferidos em AJUSTE_EXTRACAO antes do primeiro arquivo. `fila` substitui a
    conta padrao de pendentes quando a pergunta e outra (ver `extrair_tabelas`).
    """
    if fila is None:
        fila = pendentes(modalidade)
    if fila and ajuste:
        registrar_ajuste(ajuste, ferramenta.split()[0], parametros, prompt)
    linhas = []
    for arquivo_idt, uri in fila:
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            saida, status = json.dumps(ler(baixar(uri)), ensure_ascii=False), "OK"
        except Exception as erro:      # arquivo corrompido: fica registrado, a fila segue
            saida, status = None, "FALHA"
            print(f"FALHA {uri}: {erro}")
        idt = "ext_" + hashlib.sha256(
            f"{arquivo_idt}|{ferramenta}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((idt, arquivo_idt, None, None, inferencia, ferramenta, modelo, ajuste, saida,
                       round(time.perf_counter() - inicio, 3), status, agora))
        if len(linhas) == LOTE:
            gravar(linhas, modalidade, ferramenta)
            linhas = []

    if linhas or not fila:      # o que sobrou, ou o aviso de que nao havia nada
        gravar(linhas, modalidade, ferramenta)


COLUNAS = ("extracao_idt, arquivo_idt, recepcao_idt, extracao_origem_idt, inferencia_indic, "
           "ferramenta_nome, modelo_nome, prompt_versao_cod, saida_txt, extracao_segundos, "
           "status_cod, execucao_data")


def gravar(linhas, rotulo, ferramenta):
    """Uma insercao por lote: um snapshot Iceberg a cada LOTE linhas, nao um por arquivo."""
    if not linhas:
        print(f"nada a extrair em {rotulo}")
        return
    cur = conexao_trino().cursor()
    marcadores = ", ".join(["(" + ", ".join(["?"] * 12) + ")"] * len(linhas))
    cur.execute(f"INSERT INTO iceberg.bronze.extracao ({COLUNAS}) VALUES {marcadores}",
                [v for linha in linhas for v in linha])
    cur.fetchall()
    segundos = sum(l[9] for l in linhas)
    print(f"EXTRACAO: {len(linhas)} linhas de {rotulo} com {ferramenta}, "
          f"{segundos:.0f}s no total, {segundos/len(linhas):.1f}s cada "
          f"({sum(1 for l in linhas if l[10] == 'FALHA')} falhas)")


# =============================================================================
# O ajuste de cada chamada fica registrado em AJUSTE_EXTRACAO
# =============================================================================

def impressao(parametros: dict, prompt: str | None) -> tuple:
    """(parametros em JSON, hash) — a mesma conta para a DAG e para a carga manual."""
    parametros_txt = json.dumps(parametros, ensure_ascii=False, sort_keys=True)
    hash_cod = "sha256:" + hashlib.sha256(f"{parametros_txt}\n{prompt or ''}".encode()).hexdigest()
    return parametros_txt, hash_cod


def registrar_ajuste(codigo: str, ferramenta: str, parametros: dict, prompt: str | None = None):
    """Grava o ajuste desta rodada, ou PARA a tarefa se o codigo ja existe com outro conteudo.

    O prompt e montado do modelo canonico e do gazetteer a cada execucao: mudar
    um `quando:` no YAML muda o texto sem mudar o codigo da versao, e as linhas
    novas de EXTRACAO diriam uma versao que nao e a delas. Por isso a conferencia
    e pelo hash de parametros + texto:

        codigo novo                    -> grava
        mesmo codigo, mesmo hash       -> segue
        mesmo codigo, hash diferente   -> erro: troque a versao na DAG
    """
    parametros_txt, hash_cod = impressao(parametros, prompt)
    registrado = consultar("SELECT conteudo_hash_cod FROM iceberg.bronze.ajuste_extracao "
                           f"WHERE prompt_versao_cod = '{codigo}'")
    if registrado and registrado[0][0] == hash_cod:
        return
    if registrado:
        raise ValueError(f"o ajuste '{codigo}' mudou sem mudar de versao: o YAML, o gazetteer "
                         f"ou o codigo alteraram o texto ou os parametros. Troque a versao na DAG.")
    cur = conexao_trino().cursor()
    cur.execute("INSERT INTO iceberg.bronze.ajuste_extracao (prompt_versao_cod, ferramenta_nome, "
                "parametros_json_txt, prompt_txt, conteudo_hash_cod, registro_data) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [codigo, ferramenta, parametros_txt, prompt, hash_cod, datetime.now(timezone.utc)])
    cur.fetchall()
    print(f"AJUSTE_EXTRACAO: {codigo} registrado")


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


def pdfs_sem_tabela() -> list:
    """[(arquivo_idt, uri)] dos PDFs de tabela (RELPER, FOGOS) ainda nao lidos pelo Docling.

    O mesmo PDF ja tem a leitura do tesseract: reler com ferramenta nova ACRESCENTA
    uma linha em EXTRACAO, nunca altera a anterior. O INTEL fica de fora — e prosa, e
    segue pela cadeia tesseract -> modelo de linguagem.
    """
    com_extracao = """
        SELECT a.arquivo_idt, a.arquivo_uri_txt
        FROM iceberg.bronze.arquivo a
        JOIN iceberg.bronze.recepcao_bruta r ON r.arquivo_idt = a.arquivo_idt
        LEFT JOIN iceberg.bronze.extracao e
               ON e.arquivo_idt = a.arquivo_idt AND e.ferramenta_nome LIKE 'docling%'
        WHERE a.modalidade_cod = 'PDF' AND r.sistema_origem_cod IN ('RELPER', 'FOGOS')
          AND e.extracao_idt IS NULL
        ORDER BY a.arquivo_uri_txt
    """
    sem_extracao = """
        SELECT a.arquivo_idt, a.arquivo_uri_txt
        FROM iceberg.bronze.arquivo a
        JOIN iceberg.bronze.recepcao_bruta r ON r.arquivo_idt = a.arquivo_idt
        WHERE a.modalidade_cod = 'PDF' AND r.sistema_origem_cod IN ('RELPER', 'FOGOS')
        ORDER BY a.arquivo_uri_txt
    """
    return consultar(com_extracao, sem_extracao)


def ler_tabela(conteudo: bytes) -> dict:
    """Manda o PDF ao Docling e devolve as tabelas no MESMO formato da planilha:
    {"abas": {"tabela_1": [[celula, ...], ...]}}. Assim a Silver abre a grade do
    escaneado pelo caminho que ja abre a do .xlsx. Celula mesclada repete o texto
    em todas as posicoes que cobre."""
    import urllib.request
    import uuid

    fronteira = uuid.uuid4().hex
    partes = [f'--{fronteira}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode()
              for k, v in DOCLING_OPCOES.items()]
    partes.append(f'--{fronteira}\r\nContent-Disposition: form-data; name="files"; filename="documento.pdf"\r\n'
                  "Content-Type: application/pdf\r\n\r\n".encode() + conteudo + b"\r\n")
    partes.append(f"--{fronteira}--\r\n".encode())
    req = urllib.request.Request(f"{DOCLING_URL}/v1/convert/file", data=b"".join(partes),
                                 headers={"Content-Type": f"multipart/form-data; boundary={fronteira}"})
    resposta = json.loads(urllib.request.urlopen(req, timeout=600).read())
    if resposta.get("status") != "success":
        raise ValueError(f"docling: {resposta.get('status')} {resposta.get('errors')}")

    abas = {}
    for n, tabela in enumerate(resposta["document"]["json_content"].get("tables", []), start=1):
        d = tabela["data"]
        grade = [["" for _ in range(d["num_cols"])] for _ in range(d["num_rows"])]
        for c in d["table_cells"]:
            for linha in range(c["start_row_offset_idx"], c["end_row_offset_idx"]):
                for coluna in range(c["start_col_offset_idx"], c["end_col_offset_idx"]):
                    grade[linha][coluna] = c["text"]
        abas[f"tabela_{n}"] = grade
    return {"abas": abas}


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
    enquanto nao existir outra extracao que a tenha lido COM O PEDIDO ATUAL.
    Trocar a versao do pedido deixa todas pendentes de novo; a interpretacao
    antiga continua na Bronze, e a Silver usa a mais recente.
    """
    sql = f"""
        SELECT t.extracao_idt, t.arquivo_idt, t.saida_txt
        FROM iceberg.bronze.extracao t
        JOIN iceberg.bronze.arquivo a ON a.arquivo_idt = t.arquivo_idt
        LEFT JOIN iceberg.bronze.extracao filha ON filha.extracao_origem_idt = t.extracao_idt
                                               AND filha.prompt_versao_cod = '{LLM_PROMPT_VERSAO}'
        WHERE a.modalidade_cod = 'AUDIO' AND t.status_cod = 'OK'
          AND t.extracao_origem_idt IS NULL      -- so a transcricao, que leu o .wav direto;
          AND filha.extracao_idt IS NULL         -- sem isso a propria interpretacao contava como pendente
        ORDER BY t.extracao_idt
    """
    return consultar(sql)       # sem EXTRACAO nao ha transcricao nenhuma


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
        f"PRIORIDADES: {', '.join(PRIORIDADES_RADIO)}\n\n"
        "Devolva so um JSON com quatro chaves:\n"
        '  "codinome": um termo COPIADO da lista CODINOMES, ou null se nenhum foi dito\n'
        '  "referencia_local": um nome COPIADO da lista LUGARES, ou null\n'
        '  "prioridade": o termo COPIADO da lista PRIORIDADES que foi dito, ou null se nenhum foi dito\n'
        '  "texto": a mensagem sem o indicativo da estacao e sem as palavras de protocolo\n\n'
        "Regra rigida: codinome, referencia_local e prioridade so podem conter texto que exista "
        "LITERALMENTE nas listas. Se o que foi dito nao estiver na lista, devolva null. "
        "Nunca escreva coordenadas, quadriculas ou nomes proprios que nao estejam listados.\n"
    )


def extrair_texto():
    """Cada transcricao vira uma linha NOVA em EXTRACAO, que aponta para ela."""
    prompt_base = instrucao()
    pendentes_ = transcricoes_sem_interpretacao()
    print(f"transcricoes sem interpretacao: {len(pendentes_)}")
    if pendentes_:
        registrar_ajuste(LLM_PROMPT_VERSAO, "ollama", LLM_OPCOES, prompt_base)

    linhas = []
    for extracao_origem, arquivo_idt, saida_voz in pendentes_:
        texto = json.loads(saida_voz)["texto"]
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            resposta = perguntar(prompt_base + f"\nTRANSCRICAO: {texto}\n")
            json.loads(resposta)        # so aceita o que e JSON de verdade
            saida, status = resposta, "OK"
        except Exception as erro:
            saida, status = None, "FALHA"
            print(f"FALHA {extracao_origem}: {erro}")
        idt = "ext_" + hashlib.sha256(
            f"{extracao_origem}|{LLM_MODELO}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((idt, arquivo_idt, None, extracao_origem, "S", "ollama", LLM_MODELO,
                       LLM_PROMPT_VERSAO, saida, round(time.perf_counter() - inicio, 3),
                       status, agora))
        if len(linhas) == LOTE:
            gravar(linhas, "TEXTO", f"ollama/{LLM_MODELO}")
            linhas = []

    if linhas or not pendentes_:
        gravar(linhas, "TEXTO", f"ollama/{LLM_MODELO}")


def relatos_sem_interpretacao() -> list:
    """[(recepcao_idt, texto)] dos relatos em texto livre ainda nao interpretados.

    Diferente da voz: aqui NAO ha binario nem transcricao. O texto chega dentro
    do proprio registro JSON, e o elo da inferencia e o RECEPCAO_IDT.
    """
    com_extracao = """
        SELECT r.recepcao_idt, json_extract_scalar(r.conteudo_json_txt, '$.situacao')
        FROM iceberg.bronze.recepcao_bruta r
        LEFT JOIN iceberg.bronze.extracao i ON i.recepcao_idt = r.recepcao_idt
        WHERE r.sistema_origem_cod = 'C2_B' AND r.origem_uri_txt LIKE '%/relato/%'
          AND i.extracao_idt IS NULL
        ORDER BY r.recepcao_idt
    """
    sem_extracao = """
        SELECT r.recepcao_idt, json_extract_scalar(r.conteudo_json_txt, '$.situacao')
        FROM iceberg.bronze.recepcao_bruta r
        WHERE r.sistema_origem_cod = 'C2_B' AND r.origem_uri_txt LIKE '%/relato/%'
        ORDER BY r.recepcao_idt
    """
    return consultar(com_extracao, sem_extracao)


def instrucao_relato() -> str:
    """O pedido, montado inteiro a partir do modelo canonico.

    Tres coisas que faltavam na primeira versao e que o modelo agora fornece:
    o que o DOCUMENTO e (`documento:` da receita), QUAIS valores essa fonte pode
    produzir (`tipos_possiveis:`) e QUANDO cada valor se aplica (`quando:` do
    dominio). Sem elas o pedido era uma lista de codigos sem criterio, e o modelo
    escolhia pelo nome que mais lembrava o assunto.
    """
    modelo = yaml.safe_load(open(f"{CANONICO}/modelo_canonico.yaml", encoding="utf-8"))
    receita = modelo["fontes"]["C2_B"]["entidades"]["relato"]
    dominios = modelo["dominios"]

    def listar(dominio, apenas=None):
        valores = dominios[dominio]["valores"]
        return "\n".join(f"  {k:22s} {v.get('quando', v.get('rotulo', ''))}"
                          for k, v in valores.items() if apenas is None or k in apenas)

    campos = [c for c, r in receita["campos"].items() if r.get("por") == "llm"]
    # Cada lista vem AMARRADA a sua chave. Na versao anterior as duas listas
    # apareciam soltas e o modelo escreveu ROTINA — valor de prioridade — dentro
    # de TIPO_COD em 30 dos 300 relatos.
    return (
        f"Voce le RELATOS DE OBSERVADOR. {receita['documento']}\n\n"
        "Responda so um JSON com exatamente estas quatro chaves:\n\n"
        "1) \"TIPO_COD\" — o que o relato descreve. Use SO um destes:\n"
        f"{listar('tipo_evento', receita.get('tipos_possiveis'))}\n\n"
        "2) \"PRIORIDADE_COD\" — o que o relato exige de quem o recebe. Use SO um destes:\n"
        f"{listar('prioridade')}\n"
        "   Decida pela ACAO, nao pelo assunto. Exemplos:\n"
        "     'Fracao instalada em BR-154 km 15, PC operando normalmente.'  -> ROTINA\n"
        "     'Viatura da fracao atolada na via, solicito apoio.'           -> PRIORITARIO\n"
        "     'Tropa inimiga a 500 m da posicao, em aproximacao.'           -> URGENTE\n"
        "   Um relato que so informa e ROTINA. Um que pede providencia e PRIORITARIO.\n"
        "   Um que indica risco agora e URGENTE.\n\n"
        "3) \"FONTE_CONFIABILIDADE_COD\" e 4) \"INFO_CREDIBILIDADE_COD\" — juizo de\n"
        "   analista sobre QUEM relatou e sobre a informacao. O texto de um relato\n"
        "   quase nunca permite decidir: devolva null nas duas, salvo se o proprio\n"
        "   texto trouxer a avaliacao.\n\n"
        "Nunca use um valor de uma lista na chave da outra. Se o texto nao permitir\n"
        "decidir um campo, devolva null. Nunca invente valor fora dos listados.\n"
    )


def perguntar(prompt: str) -> str:
    """Uma chamada ao modelo, sem raciocinio, com a resposta forcada a JSON.

    Houve uma relato-v5 que fazia uma SEGUNDA chamada so para a prioridade, com o
    raciocinio ligado. Medido nos 22 fatos com gabarito (scripts/medir_acerto_relato.py):
    prioridade 64% -> 77%, dentro do erro de uma amostra desse tamanho, por ~12
    vezes o tempo (0,9 s -> 11 s por relato). Ficou a v3: o estudo precisa mostrar
    que o texto livre cruza com as outras fontes, nao calibrar o prompt.
    """
    import urllib.request

    corpo = json.dumps({"model": LLM_MODELO, "prompt": prompt, "stream": False,
                        **LLM_OPCOES}).encode()
    req = urllib.request.Request(f"{LLM_ENDERECO}/api/generate", data=corpo,
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=600).read())["response"]


def extrair_relatos():
    """Cada relato vira uma linha em EXTRACAO ligada ao registro, nao a um arquivo."""

    prompt_base = instrucao_relato()
    pendentes_ = relatos_sem_interpretacao()
    print(f"relatos sem interpretacao: {len(pendentes_)}")
    if pendentes_:
        registrar_ajuste(LLM_PROMPT_VERSAO_RELATO, "ollama", LLM_OPCOES, prompt_base)

    linhas = []
    for recepcao_idt, texto in pendentes_:
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            resposta = perguntar(prompt_base + f"\nRELATO: {texto}\n")
            json.loads(resposta)        # so aceita o que e JSON de verdade
            saida, status = resposta, "OK"
        except Exception as erro:
            saida, status = None, "FALHA"
            print(f"FALHA {recepcao_idt}: {erro}")
        idt = "ext_" + hashlib.sha256(
            f"{recepcao_idt}|{LLM_MODELO}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((idt, None, recepcao_idt, None, "S", "ollama", LLM_MODELO,
                       LLM_PROMPT_VERSAO_RELATO, saida,
                       round(time.perf_counter() - inicio, 3), status, agora))
        if len(linhas) == LOTE:
            gravar(linhas, "RELATO", f"ollama/{LLM_MODELO}")
            linhas = []

    if linhas or not pendentes_:
        gravar(linhas, "RELATO", f"ollama/{LLM_MODELO}")


def ocr_sem_interpretacao() -> list:
    """[(extracao_idt, arquivo_idt, saida)] das leituras de OCR do INTEL ainda nao interpretadas.

    Mesma conta da voz: uma leitura esta pendente enquanto nenhuma outra extracao
    a tiver lido. Sem EXTRACAO nao ha leitura de OCR nenhuma.
    """
    sql = """
        SELECT t.extracao_idt, t.arquivo_idt, t.saida_txt
        FROM iceberg.bronze.extracao t
        JOIN iceberg.bronze.recepcao_bruta r ON r.arquivo_idt = t.arquivo_idt
        LEFT JOIN iceberg.bronze.extracao filha ON filha.extracao_origem_idt = t.extracao_idt
        WHERE r.sistema_origem_cod = 'INTEL' AND t.status_cod = 'OK'
          AND t.extracao_origem_idt IS NULL      -- so o OCR, que leu o PDF direto
          AND filha.extracao_idt IS NULL
        ORDER BY t.extracao_idt
    """
    return consultar(sql)


def instrucao_informe() -> str:
    """O pedido para o informe, montado a partir da receita do INTEL no modelo canonico.

    Medido numa amostra dos 30 informes antes de virar tarefa (13/09): numero e
    data-hora 30/30, lugar 29/30, tipo 14/14 e prioridade 8/14 contra o gabarito.
    O lugar e pedido COMO ESTA ESCRITO, sem a lista do gazetteer: com a lista de 70
    nomes o modelo devolvia vazio em 5 informes de nome legivel. Quem transforma o
    nome em coordenada e a consulta a REF_GAZETTEER, na Silver.
    """
    modelo = yaml.safe_load(open(f"{CANONICO}/modelo_canonico.yaml", encoding="utf-8"))
    receita = modelo["fontes"]["INTEL"]["entidades"]["informe"]
    dominios = modelo["dominios"]

    def listar(dominio, apenas=None):
        return "\n".join(f"     {k:22s} {v.get('quando', v.get('rotulo', ''))}"
                         for k, v in dominios[dominio]["valores"].items() if apenas is None or k in apenas)

    return (
        "Voce le o texto de um INFORME DE INTELIGENCIA militar escaneado, reconhecido por OCR. "
        "O OCR pode errar letras. Nao invente: o que nao estiver no texto vira null.\n\n"
        f"{receita['documento']}\n\n"
        "Responda so um JSON com estas chaves:\n"
        '  "nr": o numero do informe, como no cabecalho (ex. 001/24)\n'
        '  "data_hora": o grupo data-hora do cabecalho, como escrito (ex. 250812 NOV 24)\n'
        '  "avaliacao": letra de A a F seguida de algarismo de 1 a 6 (ex. B2)\n'
        '  "referencia_local": o lugar citado no paragrafo 1, copiado EXATAMENTE como esta escrito '
        '(o nome que vem depois de "nas proximidades de"), ou null se nao houver\n'
        '  "ocorrencia": dia e hora do fato, do paragrafo 1, no formato AAAA-MM-DDTHH:MM (o ano e o do cabecalho)\n'
        '  "TIPO_COD": o que o informe relata. Use SO um destes:\n'
        + listar("tipo_evento", receita["tipos_possiveis"]) + "\n"
        '  "PRIORIDADE_COD": o que o informe exige de quem o recebe. Use SO um destes:\n'
        + listar("prioridade") + "\n\n"
    )


def extrair_informes():
    """Cada leitura de OCR de um informe vira uma linha NOVA em EXTRACAO, que aponta para ela.

    A cadeia e a da voz com outro par de modelos: o tesseract leu a pagina
    escaneada; o modelo de linguagem le ESSE TEXTO e devolve os campos.
    """
    prompt_base = instrucao_informe()
    pendentes_ = ocr_sem_interpretacao()
    print(f"informes sem interpretacao: {len(pendentes_)}")
    if pendentes_:
        registrar_ajuste(LLM_PROMPT_VERSAO_INTEL, "ollama", LLM_OPCOES, prompt_base)

    linhas = []
    for extracao_origem, arquivo_idt, saida_ocr in pendentes_:
        texto = "\n".join(json.loads(saida_ocr)["paginas"])
        inicio = time.perf_counter()
        agora = datetime.now(timezone.utc)
        try:
            resposta = perguntar(prompt_base + f"\nTEXTO DO OCR:\n{texto}\n")
            json.loads(resposta)        # so aceita o que e JSON de verdade
            saida, status = resposta, "OK"
        except Exception as erro:
            saida, status = None, "FALHA"
            print(f"FALHA {extracao_origem}: {erro}")
        idt = "ext_" + hashlib.sha256(
            f"{extracao_origem}|{LLM_MODELO}|{agora.isoformat()}".encode()).hexdigest()[:16]
        linhas.append((idt, arquivo_idt, None, extracao_origem, "S", "ollama", LLM_MODELO,
                       LLM_PROMPT_VERSAO_INTEL, saida, round(time.perf_counter() - inicio, 3),
                       status, agora))
        if len(linhas) == LOTE:
            gravar(linhas, "INFORME", f"ollama/{LLM_MODELO}")
            linhas = []

    if linhas or not pendentes_:
        gravar(linhas, "INFORME", f"ollama/{LLM_MODELO}")


def conferir_pendentes() -> str:
    quantos = {m: len(pendentes(m)) for m in ("PLANILHA", "PDF", "AUDIO")}
    quantos["transcricoes a interpretar"] = len(transcricoes_sem_interpretacao())
    quantos["relatos a interpretar"] = len(relatos_sem_interpretacao())
    quantos["informes a interpretar"] = len(ocr_sem_interpretacao())
    quantos["tabelas a ler"] = len(pdfs_sem_tabela())
    print("pendente:", quantos)
    return "planilha" if sum(quantos.values()) else "nada_a_fazer"


def extrair_planilha():
    import openpyxl
    executar("PLANILHA", ler_planilha, f"openpyxl {openpyxl.__version__}", inferencia="N")


def extrair_ocr():
    import pytesseract
    executar("PDF", ler_ocr, f"tesseract {pytesseract.get_tesseract_version()}",
             inferencia="S", modelo=OCR_IDIOMA,
             ajuste=f"psm{OCR_SEGMENTACAO}-{OCR_RESOLUCAO}dpi",
             parametros={"idioma": OCR_IDIOMA, "resolucao_dpi": OCR_RESOLUCAO,
                         "segmentacao_psm": OCR_SEGMENTACAO})


def extrair_tabelas():
    """PDFs de tabela -> grade, pelo servico Docling.

    A versao e pedida ao servico ANTES do primeiro arquivo, e isso tambem e teste de
    vida: com o servico fora do ar a tarefa fica vermelha aqui, em vez de gravar uma
    FALHA por PDF — que a conta de pendentes nunca mais tentaria reler.
    """
    import urllib.request

    fila = pdfs_sem_tabela()
    if not fila:
        print("nada a extrair em PDF de tabela")
        return
    versoes = json.loads(urllib.request.urlopen(f"{DOCLING_URL}/version", timeout=30).read())
    print(f"docling-serve {versoes['docling-serve']} | docling {versoes['docling']} | {len(fila)} PDFs")
    executar("PDF", ler_tabela, f"docling-serve {versoes['docling-serve']}", inferencia="S",
             modelo=f"docling {versoes['docling']} + rapidocr", ajuste=DOCLING_AJUSTE,
             parametros=DOCLING_OPCOES, fila=fila)


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
             modelo=f"{VOZ_MODELO}-{precisao}", ajuste=VOZ_VOCABULARIO,
             parametros={"idioma": VOZ_IDIOMA}, prompt=vocab)


def tarefa(nome, funcao, usa_ajuste=True) -> PythonOperator:
    """Toda tarefa le ARQUIVO e escreve EXTRACAO. As que tem ajuste — todas menos
    a planilha, que e leitura direta — tambem leem AJUSTE_EXTRACAO."""
    le = ["bronze.arquivo"]
    colunas = {"arquivo_idt": [("bronze.arquivo", "arquivo_idt")],
               "saida_txt":   [("bronze.arquivo", "arquivo_uri_txt")]}
    if usa_ajuste:
        le.append("bronze.ajuste_extracao")
        colunas["prompt_versao_cod"] = [("bronze.ajuste_extracao", "prompt_versao_cod")]
    return PythonOperator(
        task_id=nome,
        python_callable=funcao,
        on_success_callback=linhagem(le=le, escreve=["bronze.extracao"], colunas=colunas),
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

    planilha = tarefa("planilha", extrair_planilha, usa_ajuste=False)
    ocr = tarefa("ocr", extrair_ocr)
    tabela = tarefa("tabela", extrair_tabelas)
    voz = tarefa("voz", extrair_voz)
    texto = PythonOperator(
        task_id="texto",
        python_callable=extrair_texto,
        # a entrada e outra EXTRACAO, nao o arquivo: a linhagem e da tabela para ela mesma
        on_success_callback=linhagem(
            le=["bronze.extracao", "bronze.ajuste_extracao"], escreve=["bronze.extracao"],
            colunas={"saida_txt": [("bronze.extracao", "saida_txt")],
                     "extracao_origem_idt": [("bronze.extracao", "extracao_idt")],
                     "prompt_versao_cod": [("bronze.ajuste_extracao", "prompt_versao_cod")]},
        ),
    )

    relato = PythonOperator(
        task_id="relato",
        python_callable=extrair_relatos,
        # a entrada e o registro bruto, nao um arquivo: o relato nunca teve binario
        on_success_callback=linhagem(
            le=["bronze.recepcao_bruta", "bronze.ajuste_extracao"], escreve=["bronze.extracao"],
            colunas={"saida_txt": [("bronze.recepcao_bruta", "conteudo_json_txt")],
                     "recepcao_idt": [("bronze.recepcao_bruta", "recepcao_idt")],
                     "prompt_versao_cod": [("bronze.ajuste_extracao", "prompt_versao_cod")]},
        ),
    )

    informe = PythonOperator(
        task_id="informe",
        python_callable=extrair_informes,
        # como a `texto`: a entrada e outra EXTRACAO (o OCR do PDF), nao o arquivo
        on_success_callback=linhagem(
            le=["bronze.extracao", "bronze.ajuste_extracao"], escreve=["bronze.extracao"],
            colunas={"saida_txt": [("bronze.extracao", "saida_txt")],
                     "extracao_origem_idt": [("bronze.extracao", "extracao_idt")],
                     "prompt_versao_cod": [("bronze.ajuste_extracao", "prompt_versao_cod")]},
        ),
    )

    conferir >> [planilha, nada_a_fazer]
    planilha >> ocr >> tabela >> voz >> texto >> relato >> informe
