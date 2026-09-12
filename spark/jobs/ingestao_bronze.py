#!/usr/bin/env python3
"""
ingestao_bronze.py — cataloga o que chegou em landing/ nas tabelas Bronze
RECEPCAO_BRUTA e ARQUIVO do modelo canonico.

Le tudo que ha em s3a://lakehouse/landing/ e grava:

    .json SEM binario ao lado (C2_A, C2_B/relato) -> 1 linha de RECEPCAO_BRUTA por registro
                                                     (um lote JSON com N registros vira N linhas)
    binario (xlsx, pdf, jpg, wav)                 -> 1 linha de ARQUIVO
    .json AO LADO de um binario (o sidecar)       -> 1 linha de RECEPCAO_BRUTA apontando
                                                     para o ARQUIVO (ARQUIVO_IDT)

So cataloga. Nao abre planilha, PDF, foto nem audio para ler o CONTEUDO — isso e
trabalho das DAGs de extracao, que gravam em EXTRACAO. O que se le aqui e o que
esta ABOUT o arquivo: tamanho, tipo, hash e o metadado tecnico (EXIF da foto).

Idempotente: a chave das duas tabelas vem do hash do conteudo, e so entra o que
ainda nao esta na tabela. Rodar duas vezes nao duplica nem altera nada — a Bronze
e append-only. O mesmo arquivo reenviado com outro nome nao gera linha nova.

Uso (dentro do container, via spark-submit):
    ingestao_bronze.py [--landing s3a://lakehouse/landing/]
"""
import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

# O modelo canonico e quem define as tabelas; este job so as preenche.
from canonico_para_silver import achar_modelo, carregar_modelo, criar_tabelas

CATALOGO = "lakehouse"
TABELA_RECEPCAO = f"{CATALOGO}.bronze.RECEPCAO_BRUTA"
TABELA_ARQUIVO = f"{CATALOGO}.bronze.ARQUIVO"

# landing/<operacao>/<fonte>/... — a pasta diz de que sistema o dado veio
FONTE_POR_PASTA = {
    "c2a": "C2_A", "c2b": "C2_B", "relper": "RELPER",
    "fogos": "FOGOS", "intel": "INTEL", "voz": "VOZ",
}
MODALIDADE_POR_EXTENSAO = {
    "xlsx": "PLANILHA", "pdf": "PDF", "jpg": "IMAGEM", "jpeg": "IMAGEM",
    "png": "IMAGEM", "wav": "AUDIO",
}
MIME_POR_EXTENSAO = {
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "pdf": "application/pdf", "jpg": "image/jpeg", "jpeg": "image/jpeg",
    "png": "image/png", "wav": "audio/wav", "json": "application/json",
}
# O EXIF grava a hora local sem fuso; as fotos do estudo sao do horario de Brasilia.
FUSO_EXIF = timezone(timedelta(hours=-3))


# =============================================================================
# 1. O que se le do CAMINHO
# =============================================================================

def partes_do_caminho(uri: str):
    """s3a://lakehouse/landing/perseu_2024/c2a/posicao/x.json
       -> ('PERSEU_2024', 'C2_A', 'json')"""
    depois = uri.split("/landing/", 1)[1]           # perseu_2024/c2a/posicao/x.json
    pastas = depois.split("/")
    operacao = pastas[0].upper()
    fonte = FONTE_POR_PASTA.get(pastas[1].lower(), pastas[1].upper())
    extensao = pastas[-1].rsplit(".", 1)[-1].lower()
    return operacao, fonte, extensao


def sem_extensao(uri: str) -> str:
    return uri.rsplit(".", 1)[0]


def hash_de(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


def codigo_da_operacao(registro) -> str | None:
    """O codigo canonico e NOME_ANO. O sidecar ja traz assim ('PERSEU_2024');
    o C2_A traz a chave natural dele em dois campos ('Perseu' + '2024')."""
    if not isinstance(registro, dict) or not registro.get("operacao"):
        return None
    operacao = str(registro["operacao"]).strip().upper().replace(" ", "_")
    if registro.get("ano"):
        operacao = f"{operacao}_{registro['ano']}"
    return operacao


# =============================================================================
# 2. Uma linha de ARQUIVO para cada binario
# =============================================================================

def ler_exif(conteudo: bytes):
    """Data e GPS gravados pela camera dentro do JPEG. (None, None) se nao ha."""
    try:
        import piexif
        exif = piexif.load(conteudo)
    except Exception:
        return None, None

    data = None
    bruto = exif.get("Exif", {}).get(piexif.ExifIFD.DateTimeOriginal)
    if bruto:
        data = datetime.strptime(bruto.decode(), "%Y:%m:%d %H:%M:%S").replace(tzinfo=FUSO_EXIF)

    def graus(racionais, ref):
        # EXIF guarda (graus, minutos, segundos) como fracoes; converte para decimal
        g, m, s = (n / d for n, d in racionais)
        valor = g + m / 60 + s / 3600
        return -valor if ref in (b"S", b"W") else valor

    ponto = None
    gps = exif.get("GPS", {})
    if piexif.GPSIFD.GPSLatitude in gps and piexif.GPSIFD.GPSLongitude in gps:
        lat = graus(gps[piexif.GPSIFD.GPSLatitude], gps.get(piexif.GPSIFD.GPSLatitudeRef))
        lon = graus(gps[piexif.GPSIFD.GPSLongitude], gps.get(piexif.GPSIFD.GPSLongitudeRef))
        ponto = f"POINT({lon:.6f} {lat:.6f})"
    return data, ponto


def catalogar_binario(linha) -> dict:
    conteudo = bytes(linha.content)
    operacao, _fonte, extensao = partes_do_caminho(linha.path)
    hash_hex = hash_de(conteudo)

    captura_data, captura_wkt = (None, None)
    if extensao in ("jpg", "jpeg"):
        captura_data, captura_wkt = ler_exif(conteudo)

    # PDF sem fonte de texto embutida e imagem digitalizada: vai precisar de OCR.
    # Heuristica sobre os bytes, sem interpretar o documento.
    digitalizado = None
    if extensao == "pdf":
        digitalizado = "N" if b"/Font" in conteudo else "S"

    return {
        "ARQUIVO_IDT": "arq_" + hash_hex[:16],
        "OPERACAO_COD": operacao,                       # o palpite da pasta
        "CONTEUDO_HASH_COD": "sha256:" + hash_hex,
        "ARQUIVO_URI_TXT": linha.path,
        "MIME_TIPO_COD": MIME_POR_EXTENSAO.get(extensao, "application/octet-stream"),
        "MODALIDADE_COD": MODALIDADE_POR_EXTENSAO.get(extensao, "BINARIO"),
        "ARQUIVO_TAMANHO_QNT": int(linha.length),
        "CAPTURA_DATA": captura_data,
        "CAPTURA_GEOMETRIA_WKT": captura_wkt,
        "DIGITALIZADO_INDIC": digitalizado,
        "RECEBIMENTO_DATA": linha.modificationTime,     # quando pousou em landing/
    }


# =============================================================================
# 3. Uma linha de RECEPCAO_BRUTA para cada registro JSON
# =============================================================================

def abrir_json(linha, arquivos_por_nome: dict):
    """Devolve as linhas de RECEPCAO_BRUTA de um .json: uma por registro.
    `arquivos_por_nome` diz, para cada binario, seu ARQUIVO_IDT e modalidade —
    e assim que se descobre se este .json e o sidecar de alguem."""
    dado = json.loads(bytes(linha.content).decode("utf-8"))
    registros = dado if isinstance(dado, list) else [dado]
    _operacao_pasta, fonte, _ = partes_do_caminho(linha.path)

    arquivo_idt, modalidade = arquivos_por_nome.get(sem_extensao(linha.path), (None, "JSON"))

    linhas = []
    for registro in registros:
        # o registro cru, inteiro, como texto — nenhum campo foi lido, exceto a
        # operacao, que o modelo exige na Bronze porque e o elo entre as fontes
        texto = json.dumps(registro, ensure_ascii=False, separators=(",", ":"))
        hash_hex = hash_de(texto.encode("utf-8"))
        linhas.append({
            # A identidade da recepcao inclui o ARQUIVO que ela acompanha: receber
            # um sidecar PARA UM arquivo e diferente de receber o mesmo texto para
            # outro. Sem isso, a mesma remessa enviada em dois formatos — a
            # planilha e o escaneado assinado — colapsa numa linha so, e um dos
            # dois binarios fica sem registro de recepcao. Para JSON sem binario
            # nada muda: o ARQUIVO_IDT e vazio.
            "RECEPCAO_IDT": "rcp_" + hash_de(f"{fonte}|{hash_hex}|{arquivo_idt or ''}".encode())[:16],
            "SISTEMA_ORIGEM_COD": fonte,
            "OPERACAO_COD": codigo_da_operacao(registro),
            "MODALIDADE_COD": modalidade,
            "CONTEUDO_JSON_TXT": texto,
            "CONTEUDO_HASH_COD": "sha256:" + hash_hex,
            "ORIGEM_URI_TXT": linha.path,
            "ARQUIVO_IDT": arquivo_idt,
            "RECEBIMENTO_DATA": linha.modificationTime,
        })
    return linhas


# =============================================================================
# 4. Gravar so o que ainda nao esta na tabela
# =============================================================================

def gravar_novos(spark, rdd_de_dicts, tabela: str, chave: str) -> int:
    """Converte os dicionarios no esquema da tabela, tira o que ja existe (pela
    chave) e faz append. Devolve quantas linhas entraram."""
    esquema = spark.table(tabela).schema
    colunas = [c.name for c in esquema]
    df = spark.createDataFrame(
        rdd_de_dicts.map(lambda d: tuple(d[c.upper()] for c in colunas)), esquema
    )
    existentes = spark.table(tabela).select(chave)
    novos = df.dropDuplicates([chave]).join(existentes, chave, "left_anti").cache()
    n = novos.count()
    if n:
        novos.writeTo(tabela).append()
    novos.unpersist()
    return n


# =============================================================================
# 5. O job
# =============================================================================

def get_spark():
    return (
        SparkSession.builder
        .appName("ingestao_bronze")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # O leitor VETORIZADO do Iceberg (Arrow, memoria fora do heap da JVM)
        # derruba o executor sem excecao Java — codigo 134 — ao reler a tabela
        # durante um MERGE que atualiza linhas. Desligado: a leitura fica um
        # pouco mais lenta e nao quebra. Diagnosticado em 12/09/2026.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .getOrCreate()
    )


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--landing", default="s3a://lakehouse/landing/")
    a = p.parse_args()

    spark = get_spark()

    # as tabelas vem do modelo canonico; criar e idempotente
    modelo = carregar_modelo(achar_modelo(), validar_mapeamentos=False)
    criar_tabelas(spark, modelo)

    # tudo que ha em landing/, como bytes: caminho, tamanho, hora de chegada, conteudo
    tudo = (spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .load(a.landing))
    binarios = tudo.filter(~tudo.path.endswith(".json"))
    jsons = tudo.filter(tudo.path.endswith(".json"))

    # 1) os binarios viram ARQUIVO
    arquivos = binarios.rdd.map(catalogar_binario).cache()
    n_arquivos = gravar_novos(spark, arquivos, TABELA_ARQUIVO, "ARQUIVO_IDT")

    # 2) quem e sidecar de quem: mesmo caminho sem a extensao
    arquivos_por_nome = {
        sem_extensao(d["ARQUIVO_URI_TXT"]): (d["ARQUIVO_IDT"], d["MODALIDADE_COD"])
        for d in arquivos.collect()
    }
    arquivos.unpersist()

    # 3) os .json viram RECEPCAO_BRUTA (o sidecar ja sai apontando para o ARQUIVO)
    recepcoes = jsons.rdd.flatMap(lambda linha: abrir_json(linha, arquivos_por_nome))
    n_recepcoes = gravar_novos(spark, recepcoes, TABELA_RECEPCAO, "RECEPCAO_IDT")

    print(f"landing: {binarios.count()} binarios, {jsons.count()} json")
    print(f"novas linhas: ARQUIVO={n_arquivos}  RECEPCAO_BRUTA={n_recepcoes}")
    print(f"total: ARQUIVO={spark.table(TABELA_ARQUIVO).count()}  "
          f"RECEPCAO_BRUTA={spark.table(TABELA_RECEPCAO).count()}")
    spark.stop()


if __name__ == "__main__":
    main()
