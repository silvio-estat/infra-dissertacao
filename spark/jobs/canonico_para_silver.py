"""
canonico_para_silver.py — Le canonico/modelo_canonico.yaml e materializa a Silver.

NAO existe codigo por fonte aqui. Nao ha `if fonte == 'C2_A'`. O programa le o
de/para declarado no modelo e monta o SQL correspondente.

    Como funciona, em quatro passos:

    1. carrega o modelo e VALIDA (campo inexistente ou transformacao desconhecida
       derrubam o job agora, no carregamento, e nao no meio da execucao);
    2. cria as tabelas a partir da secao `entidades`, com os comentarios de coluna;
    3. carrega os seeds nas tabelas de referencia;
    4. para cada fonte, monta um SELECT a partir do de/para e faz MERGE em EVENTO.

    Decisao de projeto: toda transformacao devolve uma EXPRESSAO SQL, nunca uma
    funcao Python. O job monta a consulta e o Spark executa. Isso da tres coisas:
    a consulta gerada e exatamente o que vai para a linhagem; nao ha custo de
    serializacao; e `--mostrar-sql` permite ver o que o modelo produziu.

    python3 canonico_para_silver.py --ddl
    python3 canonico_para_silver.py --seeds
    python3 canonico_para_silver.py --fonte SIM --tipo gps
    python3 canonico_para_silver.py --fonte SIM --tipo gps --mostrar-sql
    python3 canonico_para_silver.py --listar
"""

import argparse
import re
import unicodedata
import os
import sys
from pathlib import Path

import yaml

CATALOGO = "lakehouse"

# O modelo e procurado em varios lugares porque o job roda em dois contextos:
# na maquina do desenvolvedor (--mostrar-sql, sem Spark) e dentro do container,
# onde canonico/ e montado em /opt/canonico.
_CANDIDATOS = [
    Path(__file__).resolve().parents[2] / "canonico" / "modelo_canonico.yaml",
    Path("/opt/canonico/modelo_canonico.yaml"),
]


def achar_modelo(indicado: str | None = None) -> Path:
    if indicado:
        return Path(indicado)
    for c in [Path(os.environ["MODELO_CANONICO"])] if os.environ.get("MODELO_CANONICO") else []:
        return c
    for c in _CANDIDATOS:
        if c.exists():
            return c
    raise SystemExit(
        "modelo canonico nao encontrado. Procurei em:\n  "
        + "\n  ".join(str(c) for c in _CANDIDATOS)
        + "\nUse --modelo CAMINHO ou defina MODELO_CANONICO."
    )


class ModeloInvalido(RuntimeError):
    """O modelo canonico declara algo que nao existe."""


class TransformacaoPendente(NotImplementedError):
    """Transformacao declarada no modelo, mas ainda sem implementacao."""


# =============================================================================
# 1. CARGA E VALIDACAO DO MODELO
# =============================================================================

def carregar_modelo(caminho: Path, validar_mapeamentos: bool = True) -> dict:
    """Le o YAML. A validacao do de/para so e exigida para PROCESSAR uma fonte:
    criar tabelas e carregar seeds dependem apenas de `entidades`, e nao devem
    ficar reféns de uma transformacao ainda nao implementada."""
    modelo = yaml.safe_load(caminho.read_text(encoding="utf-8"))
    if validar_mapeamentos:
        validar(modelo)
    # os seeds sao relativos ao proprio modelo, e nao a raiz do repositorio
    modelo["_dir"] = caminho.parent
    return modelo


def validar(modelo: dict) -> None:
    """Falha AGORA se o de/para citar algo que nao existe."""
    entidades, dominios = modelo["entidades"], modelo["dominios"]
    colunas = {c for e in entidades.values() for c in e["campos"]}
    problemas = []

    for fonte, spec in modelo["fontes"].items():
        for bloco, mapa in _blocos_de_mapeamento(spec):
            for origem, regra in mapa.items():
                if not isinstance(regra, dict) or "campo" not in regra:
                    continue
                onde = f"{fonte}.{bloco}.{origem}"
                for alvo in _lista(regra["campo"]):
                    if alvo.split(".")[-1] not in colunas:
                        problemas.append(f"{onde}: coluna inexistente '{alvo}'")
                if regra["transformacao"] not in TRANSFORMACOES:
                    problemas.append(f"{onde}: transformacao desconhecida '{regra['transformacao']}'")
                if regra.get("dominio") and regra["dominio"] not in dominios:
                    problemas.append(f"{onde}: dominio inexistente '{regra['dominio']}'")

    if problemas:
        raise ModeloInvalido("modelo canonico invalido:\n  " + "\n  ".join(problemas))


def _lista(v):
    return v if isinstance(v, list) else [v]


def _mapa_do_bloco(bloco: dict) -> dict:
    """As tres partes do de/para de uma entidade, na ordem em que se sobrepoem:
    o sidecar (o que e ABOUT o arquivo), os campos (o evento) e a extensao
    (as colunas que so esta fonte tem). Chave repetida: vale a ultima.

    `sidecar:` as vezes e so uma frase — quando o proprio registro JSON da fonte
    ja faz esse papel (o incidente do C2_B acompanha a foto). Nesse caso nao ha
    campos a mapear a partir dele.
    """
    partes = [bloco.get(nome) for nome in ("sidecar", "campos", "extensao")]
    return {c: r for parte in partes if isinstance(parte, dict) for c, r in parte.items()}


def _blocos_de_mapeamento(spec: dict):
    """Devolve (nome_do_bloco, mapa_de_campos) para todas as formas que uma fonte assume."""
    if spec.get("comum"):
        yield "comum", spec["comum"]
    for nome, mapa in (spec.get("tipos") or {}).items():
        yield nome, mapa
    for nome, ent in (spec.get("entidades") or {}).items():
        yield nome, _mapa_do_bloco(ent)


# =============================================================================
# 2. TRANSFORMACOES — o unico lugar do projeto com codigo por conversao
# =============================================================================
# Cada funcao recebe o nome da coluna de origem e a regra declarada no modelo, e
# devolve uma EXPRESSAO SQL. `ctx` traz o modelo inteiro, para as transformacoes
# que precisam consultar dominios.

def _expressao_origem(origem, regra, ctx):
    """De onde o valor vem, ANTES de qualquer conversao.

    As transformacoes compoem: primeiro resolve-se a origem (coluna, caminho
    dentro do JSON, celula da planilha ou campo do sidecar), e so depois a
    conversao e aplicada por cima. Sem isso, `dominio` sobre um campo que mora
    dentro do payload seria aplicado a uma coluna que nao existe.

    O nome da chave no de/para diz de onde ler:
      _alguma_coisa   pseudo-campo: nao existe na origem (constante ou derivado)
      no bloco sidecar -> SIDECAR['operacao'], o .json que acompanha o arquivo
      qualquer outro   -> CELULAS['Ef Pres'], o cabecalho como esta na planilha
    """
    if regra.get("caminho"):
        return f"get_json_object({ctx['coluna_payload']}, '{regra['caminho']}')"
    if origem in ctx.get("sidecar", ()):
        return _registrar(ctx, "sc", origem, f"SIDECAR['{origem}']")
    if origem.startswith("_"):
        return "NULL"       # pseudo-campo: nao existe na origem
    if ctx.get("de_grade"):
        # `grafias` lista outros rotulos para a MESMA medida: a revisao do
        # formulario renomeou a coluna, mas o significado nao mudou. Vale o
        # primeiro que a planilha tiver.
        nomes = [origem] + list(regra.get("grafias") or [])
        apelidos = [_registrar(ctx, "cel", n, f"CELULAS['{n}']") for n in nomes]
        return apelidos[0] if len(apelidos) == 1 else f"coalesce({', '.join(apelidos)})"
    return origem


def _registrar(ctx, prefixo, nome, expressao) -> str:
    """Guarda a leitura do mapa para virar coluna simples, e devolve o apelido.

    Por que nao usar CELULAS['Ef Pres'] direto na expressao final: o Spark recusa
    subconsulta correlacionada que aponte para uma coluna do tipo mapa — e
    `referencia`, `gazetteer` e `abreviatura` sao exatamente isso. Resolvendo as
    celulas numa camada de baixo, o que chega as consultas e texto comum.
    """
    limpo = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode()
    apelido = f"{prefixo}_" + re.sub(r"[^0-9a-zA-Z]+", "_", limpo).strip("_").lower()
    ctx.setdefault("origens", {})[apelido] = expressao
    return apelido


def _t_direto(origem, regra, ctx, alvo=None):
    return origem


def _t_constante(origem, regra, ctx, alvo=None):
    return f"'{regra['valor']}'"


def _t_json(origem, regra, ctx, alvo=None):
    # o caminho JSON ja foi resolvido por _expressao_origem; aqui so repassa
    return origem


def _t_ts_iso(origem, regra, ctx, alvo=None):
    return f"CAST({origem} AS TIMESTAMP)"


def _t_ts_br(origem, regra, ctx, alvo=None):
    formato = regra.get("formato", "dd/MM/yyyy HH:mm:ss")
    return f"to_timestamp({origem}, '{formato}')"


def _t_ponto_de_latlon(origem, regra, ctx, alvo=None):
    """Par de numeros -> POINT(lon lat). WKT poe longitude primeiro."""
    lat = f"CAST(get_json_object({ctx['coluna_payload']}, '{regra['caminho_lat']}') AS DOUBLE)"
    lon = f"CAST(get_json_object({ctx['coluna_payload']}, '{regra['caminho_lon']}') AS DOUBLE)"
    return (
        f"CASE WHEN {lat} BETWEEN -90 AND 90 AND {lon} BETWEEN -180 AND 180 "
        f"THEN concat('POINT(', {lon}, ' ', {lat}, ')') END"
    )


def _t_dominio(origem, regra, ctx, alvo=None):
    """Valor da origem -> valor canonico, pelos sinonimos declarados no modelo.

    Vira um CASE WHEN. Valor que nao casa com nada devolve NULL, e o job reporta
    quantos foram — e essa a promessa do dominio controlado: nao passar em silencio.
    """
    valores = ctx["modelo"]["dominios"][regra["dominio"]]["valores"]
    ramos = []
    for canonico, definicao in valores.items():
        aceitos = {str(canonico)} | {str(s) for s in (definicao.get("sinonimos") or [])}
        lista = ", ".join("'" + a.lower().replace("'", "''") + "'" for a in sorted(aceitos))
        ramos.append(f"WHEN lower(trim({origem})) IN ({lista}) THEN '{canonico}'")
    return "CASE " + " ".join(ramos) + " END"


def _t_split_escala(origem, regra, ctx, alvo=None):
    """Um campo de origem, DOIS canonicos.

    A doutrina escreve o grau de confianca num codigo unico de duas posicoes:
    'B2' = fonte geralmente confiavel, informacao provavelmente verdadeira. Aqui
    ele se separa em duas colunas, e por isso a transformacao precisa saber qual
    das duas esta produzindo.
    """
    posicao = 1 if str(alvo).endswith("CONFIABILIDADE_COD") else 2
    return f"substring(trim({origem}), {posicao}, 1)"


def _comparavel(expressao: str) -> str:
    """Deixa dois textos comparaveis: sem acento, sem marca de ordinal e sem a
    letra que a acompanha. '1ª Cia', '1a Cia' e '1 Cia' viram todas '1 cia'."""
    sem_acento = f"translate(lower(trim({expressao})), 'ªº°áàâãéêíóôõúüç', 'aooaaaaeeiooouuc')"
    return f"regexp_replace(regexp_replace({sem_acento}, '([0-9]+)[ao]\\\\b', '$1'), ' +', ' ')"


def _t_referencia(origem, regra, ctx, alvo=None):
    """Consulta a uma tabela de referencia. Subconsulta escalar: continua sendo
    expressao, e por isso encaixa em qualquer lugar do SELECT. Precisa ser
    AGREGADA (max): o Spark recusa subconsulta correlacionada com LIMIT.

    Com `contexto:` declarado, a busca deixa de ser por igualdade e passa a ser
    "a sigla COMECA pelo que foi digitado", restrita ao contexto — usado na
    celula preenchida a mao, onde '1a Cia' identifica unidades diferentes em
    formularios de OM diferentes.
    """
    referida = regra["referencia"]
    tabela, coluna = (referida.split(".", 1) + [None])[:2] if "." in referida else (referida, None)
    entidade = ctx["modelo"]["entidades"][tabela]
    chave = entidade["chave"][0]
    devolve = coluna or chave
    casa = "UNIDADE_SIGLA" if tabela == "REF_UNIDADE" else chave

    if not regra.get("contexto"):
        return (f"(SELECT max(r.{devolve}) FROM {CATALOGO}.silver.{tabela} r "
                f"WHERE lower(trim(r.{casa})) = lower(trim({origem})))")

    contexto = _registrar(ctx, "sc", regra["contexto"], f"SIDECAR['{regra['contexto']}']")
    sigla, digitado, da_om = _comparavel(f"r.{casa}"), _comparavel(origem), _comparavel(contexto)
    return (
        f"(SELECT max(r.{devolve}) FROM {CATALOGO}.silver.{tabela} r "
        f"WHERE {sigla} LIKE concat({digitado}, '%') "
        f"AND ({sigla} LIKE concat('%', {da_om}) OR {sigla} = {da_om}))"
    )


def _t_gazetteer(origem, regra, ctx, alvo=None):
    """Referencia textual -> ponto. Nao converte: CONSULTA o indice de nomes."""
    filtro = ""
    if (regra.get("parametros") or {}).get("tipo"):
        filtro = f"AND g.LOCAL_TIPO_COD = '{regra['parametros']['tipo']}' "
    return (
        f"(SELECT max(g.GEOMETRIA_WKT) FROM {CATALOGO}.silver.REF_GAZETTEER g "
        f"WHERE lower(trim(g.REFERENCIA_TEXTO)) = lower(trim({origem})) {filtro})"
    )


def _t_celula(origem, regra, ctx, alvo=None):
    """Le uma celula pelo cabecalho decretado no formulario.

    Irma da `json`: a origem ja foi resolvida para CELULAS['<cabecalho>'] — a
    grade da planilha virou um mapa cabecalho -> valor antes de chegar aqui.
    Cabecalho que a planilha nao tem devolve NULL, e nao erro: uma OM que usa
    formulario antigo perde a coluna, nao a remessa inteira.
    """
    return origem


def _t_abreviatura(origem, regra, ctx, alvo=None):
    """Abreviatura digitada -> termo, consultando o dicionario do MD33-M-02.

    O que nao casa e PRESERVADO como veio ('Mun 7,62' nao e uma abreviatura
    unica), nunca descartado: a promessa e resolver o que da e deixar o resto
    visivel. 'Fz' -> Fuzil e 'Fuz' -> Fuzileiro sao entradas distintas, e e por
    isso que a troca de uma pela outra vira erro semantico detectavel.
    """
    return (
        f"COALESCE((SELECT max(a.TERMO_NOME) FROM {CATALOGO}.silver.REF_ABREVIATURA a "
        f"WHERE lower(trim(a.ABREVIATURA_COD)) = lower(trim({origem}))), {origem})"
    )


def _t_sobra(origem, regra, ctx, alvo=None):
    """Toda coluna que o formulario nao previa, com o nome que a OM deu.

    Recolhida ao abrir a grade (nao da para saber em SQL o que 'sobrou'), chega
    aqui como um JSON pronto. E o argumento de schema-on-read numa celula: a
    coluna 'Obs' que uma OM acrescentou por conta propria fica preservada sem
    nunca ter sido modelada.
    """
    return "SOBRA_JSON"


def _t_derivado(origem, regra, ctx, alvo=None):
    """Campo calculado a partir dos outros. A regra de cada um esta declarada no
    modelo, no campo `regra`/`observacao`; aqui esta o calculo correspondente."""
    if alvo == "REGISTRO_ORIGEM_COD":
        # o que identifica a observacao na origem: a REMESSA (tudo do sidecar)
        # mais a LINHA (a unidade daquela linha da planilha).
        partes = [_registrar(ctx, "sc", c, f"SIDECAR['{c}']") for c in ctx.get("sidecar", ())]
        partes.append(ctx.get("coluna_unidade") or "NULL")
        return f"concat_ws('-', {', '.join(partes)})"
    if alvo == "FUNCAO_C2_INDIC":
        # S quando o dado nasceu DENTRO de um sistema de C2; N quando o documento
        # foi apenas remetido ao repositorio, como e o caso do formulario.
        return "'S'" if ctx["fonte"] in ("C2_A", "C2_B") else "'N'"
    if alvo == "FORMULARIO_VERSAO_COD":
        return "FORMULARIO_VERSAO"          # calculada ao abrir a grade
    if alvo == "CHEGADA_DATA":
        return "RECEBIMENTO_DATA"           # quando a Bronze recebeu o arquivo
    raise TransformacaoPendente(f"derivado sem regra implementada para {alvo}")


def _pendente(fase):
    def _f(origem, regra, ctx, alvo=None):
        raise TransformacaoPendente(
            f"transformacao implementada na {fase}, quando houver dado para testar"
        )
    return _f


TRANSFORMACOES = {
    "direto":          _t_direto,
    "constante":       _t_constante,
    "json":            _t_json,
    "ts_iso":          _t_ts_iso,
    "ts_br":           _t_ts_br,
    "ponto_de_latlon": _t_ponto_de_latlon,
    "dominio":         _t_dominio,
    "referencia":      _t_referencia,
    "gazetteer":       _t_gazetteer,
    "split_escala":    _t_split_escala,
    "celula":          _t_celula,
    "abreviatura":     _t_abreviatura,
    "sobra":           _t_sobra,
    "derivado":        _t_derivado,
    # Declaradas no modelo, sem implementacao ainda: nao ha dado para exercita-las.
    # O job so falha se voce tentar processar a fonte que as usa.
    "geojson_para_wkt":       _pendente("Fase 1 (C2_A)"),
    "dms_para_ponto":         _pendente("Fase 3 (C2_B)"),
    "decametrica_para_ponto": _pendente("Fase 4 (FOGOS)"),
}


# =============================================================================
# 3. DDL — as tabelas saem da secao `entidades`
# =============================================================================

_TIPOS = {"string": "STRING", "timestamp": "TIMESTAMP", "double": "DOUBLE",
          "int": "INT", "bigint": "BIGINT", "array<string>": "ARRAY<STRING>"}


def criar_tabelas(spark, modelo, mostrar=False):
    for nome, entidade in modelo["entidades"].items():
        esquema = entidade["camada"]
        tabela = f"{CATALOGO}.{esquema}.{nome}"
        colunas = ",\n  ".join(
            f"{c} {_TIPOS[spec['tipo']]}" for c, spec in entidade["campos"].items()
        )
        ddl = (f"CREATE TABLE IF NOT EXISTS {tabela} (\n  {colunas}\n) USING iceberg")
        if mostrar:
            print(ddl + ";\n")
            continue
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{esquema}")
        spark.sql(ddl)
        # CREATE IF NOT EXISTS nao toca numa tabela que ja existe. Se ela ficou de
        # uma versao anterior do modelo, as colunas nao batem — falha aqui, com o
        # motivo, em vez de num ALTER COLUMN obscuro logo abaixo.
        existentes = [c.lower() for c in spark.table(tabela).columns]
        esperadas = [c.lower() for c in entidade["campos"]]
        if existentes != esperadas:
            raise ModeloInvalido(
                f"{tabela} ja existe com colunas diferentes do modelo.\n"
                f"  na tabela: {existentes}\n  no modelo: {esperadas}\n"
                f"  Se e residuo de uma versao anterior, apague-a (DROP TABLE) e rode de novo.")
        # Art. 137 do IR 14-06: descricao e metadado minimo de tabela e de coluna.
        spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES "
                  f"('comment' = '{_limpar(entidade['descricao'])}')")
        for coluna, spec in entidade["campos"].items():
            if spec.get("descricao"):
                spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} "
                          f"COMMENT '{_limpar(spec['descricao'])}'")
        print(f"  {tabela}: {len(entidade['campos'])} colunas")


def _limpar(texto):
    return " ".join(str(texto).split()).replace("'", "")


# =============================================================================
# 4. SEEDS — as tabelas de referencia recebem suas linhas
# =============================================================================

def carregar_seeds(spark, modelo):
    for nome, entidade in modelo["entidades"].items():
        if not entidade.get("seed"):
            continue
        caminho = modelo["_dir"] / entidade["seed"]
        linhas = [l for l in caminho.read_text(encoding="utf-8").splitlines()
                  if not l.startswith("#")]
        import csv, io
        registros = list(csv.DictReader(io.StringIO("\n".join(linhas))))
        colunas = list(entidade["campos"])
        # O seed pode vir de fora com outros nomes de coluna (o dicionario do MD33,
        # por exemplo). `seed_colunas` no modelo diz a correspondencia.
        de_para = entidade.get("seed_colunas") or {}
        registros = [{c: r.get(de_para.get(c, c)) for c in colunas} for r in registros]
        faltando = [c for c in colunas if registros[0][c] is None and c == colunas[0]]
        if faltando:
            raise ModeloInvalido(f"{caminho.name}: nao achei a coluna de {faltando}")
        # O CSV so tem texto. As colunas precisam ser convertidas para o tipo
        # declarado no modelo, senao a insercao falha por incompatibilidade.
        df = spark.createDataFrame(
            [[r[c] or None for c in colunas] for r in registros], schema=colunas
        )
        for coluna, spec in entidade["campos"].items():
            if spec["tipo"] != "string":
                df = df.withColumn(coluna, df[coluna].cast(_TIPOS[spec["tipo"]]))
        tabela = f"{CATALOGO}.{entidade['camada']}.{nome}"
        df.write.mode("overwrite").insertInto(tabela)
        print(f"  {tabela}: {len(registros)} linhas carregadas")


# =============================================================================
# 5. A ORIGEM — da Bronze para UMA LINHA POR OBSERVACAO
# =============================================================================
# As fontes que ja chegam estruturadas (C2_A, C2_B) tem uma linha de
# RECEPCAO_BRUTA por registro: cada linha ja e uma observacao. Uma planilha nao:
# ela e uma GRADE, e uma remessa so traz varias fracoes. Esta secao abre a grade
# — usando o que a leitura gravou em EXTRACAO.SAIDA_TXT — e devolve uma linha por
# fracao, com as celulas num mapa cabecalho -> valor. Dai para a frente o de/para
# volta a ser SQL, igual ao das fontes estruturadas.

def _abrir_grade(registro, cabecalhos, principais):
    """Uma linha de EXTRACAO (uma planilha) -> N linhas, uma por observacao.

    A linha de cabecalho e PROCURADA, nunca fixada: e a primeira que contem pelo
    menos metade dos cabecalhos decretados. Acima dela ficam titulo, operacao,
    OM e data — que sao do documento, nao das observacoes.
    """
    import json as _json
    from pyspark.sql import Row

    celulas_por_aba = _json.loads(registro["SAIDA_TXT"])["abas"]
    sidecar = _json.loads(registro["SIDECAR_JSON"])
    declarados, decretados = set(cabecalhos), set(principais)
    saida = []

    for grade in celulas_por_aba.values():
        titulos, primeira = None, 0
        for i, linha in enumerate(grade):
            presentes = {str(c).strip() for c in linha if c is not None}
            if len(presentes & declarados) >= len(declarados) / 2:
                titulos, primeira = [str(c).strip() if c is not None else "" for c in linha], i + 1
                break
        if titulos is None:
            continue                      # aba sem o formulario: um anexo, uma nota

        # V1 quando a planilha traz todos os cabecalhos decretados; V2 quando
        # falta algum, porque a revisao do formulario renomeou colunas. So os
        # rotulos PRINCIPAIS contam: a grafia alternativa e justamente o sinal de
        # que a planilha e da outra versao. Coluna a mais nao muda a versao — vai
        # para a sobra.
        versao = "V1" if decretados <= set(titulos) else "V2"

        for linha in grade[primeira:]:
            valores = {t: v for t, v in zip(titulos, linha) if t and v is not None}
            if not valores:
                continue                  # linha em branco: fim da tabela
            saida.append(Row(
                ARQUIVO_IDT=registro["ARQUIVO_IDT"],
                RECEPCAO_IDT=registro["RECEPCAO_IDT"],
                EXTRACAO_IDT=registro["EXTRACAO_IDT"],
                RECEBIMENTO_DATA=registro["RECEBIMENTO_DATA"],
                SIDECAR={k: (None if v is None else str(v)) for k, v in sidecar.items()},
                CELULAS={k: str(v) for k, v in valores.items() if k in declarados},
                SOBRA_JSON=_sobra_como_json(valores, declarados),
                FORMULARIO_VERSAO=versao,
            ))
    return saida


def _sobra_como_json(valores: dict, declarados: set):
    """O que a OM acrescentou por conta propria. Vazio e None, nao '{}': a sobra
    tem de ser a excecao visivel, e nao uma coluna preenchida em toda linha."""
    import json as _json
    sobra = {c: v for c, v in valores.items() if c not in declarados}
    return _json.dumps(sobra, ensure_ascii=False) if sobra else None


def montar_origem(spark, modelo, fonte, tipo):
    """Cria a view `origem_bruta`, com uma linha por observacao.

    Junta as tres tabelas da Bronze que contam a historia de um arquivo:
    o que foi LIDO dele (EXTRACAO), o arquivo em si (ARQUIVO) e o que veio
    ESCRITO ao lado dele (RECEPCAO_BRUTA, o sidecar). Os tres identificadores
    seguem junto: sao os elos de linhagem que EVENTO vai guardar.
    """
    bloco = modelo["fontes"][fonte]["entidades"][tipo]
    do_sidecar = bloco.get("sidecar") if isinstance(bloco.get("sidecar"), dict) else {}
    mapa = _mapa_do_bloco(bloco)
    principais = [c for c in mapa if not c.startswith("_") and c not in do_sidecar]
    # as grafias alternativas tambem sao cabecalhos declarados: sem isso elas
    # cairiam na sobra, como se a OM tivesse inventado a coluna. Mas nao entram
    # no conjunto que identifica a VERSAO do formulario — ver _abrir_grade.
    cabecalhos = principais + [g for c in principais for g in (mapa[c].get("grafias") or [])]

    grade = spark.sql(f"""
        SELECT e.EXTRACAO_IDT, e.SAIDA_TXT, a.ARQUIVO_IDT,
               r.RECEPCAO_IDT, r.CONTEUDO_JSON_TXT AS SIDECAR_JSON, r.RECEBIMENTO_DATA
        FROM {CATALOGO}.bronze.EXTRACAO e
        JOIN {CATALOGO}.bronze.ARQUIVO a ON a.ARQUIVO_IDT = e.ARQUIVO_IDT
        JOIN {CATALOGO}.bronze.RECEPCAO_BRUTA r ON r.ARQUIVO_IDT = a.ARQUIVO_IDT
        WHERE a.MODALIDADE_COD = '{bloco["modalidade"]}'
          AND r.SISTEMA_ORIGEM_COD = '{fonte}'
          AND e.STATUS_COD = 'OK'
    """)
    if grade.rdd.isEmpty():
        raise SystemExit(f"\nnenhuma extracao de {fonte}/{tipo} na Bronze — rode a DAG de extracao antes\n")

    linhas = grade.rdd.flatMap(lambda r: _abrir_grade(r, cabecalhos, principais))
    spark.createDataFrame(linhas).createOrReplaceTempView("origem_bruta")
    print(f"  {grade.count()} arquivos -> {spark.table('origem_bruta').count()} observacoes")


# =============================================================================
# 6. O DE/PARA VIRA UM SELECT
# =============================================================================

def montar_select(modelo, fonte, tipo=None):
    """Monta a consulta que le a origem e devolve colunas canonicas.

    Projeta as colunas de EVENTO e, quando a fonte tem extensao, tambem as dela —
    numa consulta so, porque as duas tabelas compartilham o EVENTO_IDT.
    """
    spec = modelo["fontes"][fonte]
    blocos = spec.get("entidades") or {}
    de_grade = bool(blocos)

    if de_grade:
        if tipo is None and len(blocos) == 1:
            tipo = next(iter(blocos))
        if tipo not in blocos:
            raise ModeloInvalido(f"{fonte}: informe --tipo (opcoes: {list(blocos)})")
        mapa = _mapa_do_bloco(blocos[tipo])
        declarado = blocos[tipo].get("sidecar")
        sidecar = tuple(declarado) if isinstance(declarado, dict) else ()
    else:
        mapa = dict(spec.get("comum") or {})
        mapa.update((spec.get("tipos") or {})[tipo])
        sidecar = ()

    ctx = {"modelo": modelo, "fonte": fonte, "sidecar": sidecar, "de_grade": de_grade,
           "origens": {}, "coluna_payload": spec.get("coluna_payload", "payload")}
    # de qual celula sai a unidade da linha — o derivado de REGISTRO_ORIGEM_COD precisa
    unidade = next((o for o, r in mapa.items()
                    if "UNIDADE_REPORTANTE_COD" in _lista(r["campo"]) and not o.startswith("_")), None)
    ctx["coluna_unidade"] = _registrar(ctx, "cel", unidade, f"CELULAS['{unidade}']") if unidade else None

    projecoes = {}
    for origem, regra in mapa.items():
        entrada = _expressao_origem(origem, regra, ctx)
        for alvo in _lista(regra["campo"]):
            coluna = alvo.split(".")[-1]
            projecoes[coluna] = TRANSFORMACOES[regra["transformacao"]](entrada, regra, ctx, coluna)

    # os elos de linhagem nao se "resolvem": ja vieram da Bronze prontos
    for elo in ("ARQUIVO_IDT", "RECEPCAO_IDT", "EXTRACAO_IDT"):
        if elo in projecoes and de_grade:
            projecoes[elo] = elo

    # --- campos derivados: calculados a partir dos ja mapeados, nao vem da origem
    if {"SISTEMA_ORIGEM_COD", "REGISTRO_ORIGEM_COD", "OCORRENCIA_DATA"} <= set(projecoes):
        projecoes["EVENTO_IDT"] = (
            "sha2(concat_ws('|', {SISTEMA_ORIGEM_COD}, {REGISTRO_ORIGEM_COD}, "
            "CAST({OCORRENCIA_DATA} AS STRING)), 256)".format(**projecoes)
        )
    if {"CHEGADA_DATA", "OCORRENCIA_DATA"} <= set(projecoes):
        projecoes["LATENCIA_INGESTAO_SEGUNDOS"] = (
            "CAST(unix_timestamp({CHEGADA_DATA}) - unix_timestamp({OCORRENCIA_DATA}) "
            "AS DOUBLE)".format(**projecoes)
        )

    destinos = [modelo["entidades"]["EVENTO"]["campos"]]
    if spec.get("extensao"):
        destinos.append(modelo["entidades"][spec["extensao"]]["campos"])

    select, ja = [], set()
    for campos in destinos:
        for coluna, spec_col in campos.items():
            if coluna in ja:
                continue
            ja.add(coluna)
            tipo_sql = _TIPOS[spec_col["tipo"]]
            expressao = projecoes.get(coluna, "NULL")
            select.append(f"CAST({expressao} AS {tipo_sql}) AS {coluna}")

    if de_grade:
        # camada de baixo: cada celula e cada campo do sidecar vira coluna simples
        fixas = ["ARQUIVO_IDT", "RECEPCAO_IDT", "EXTRACAO_IDT", "RECEBIMENTO_DATA",
                 "SOBRA_JSON", "FORMULARIO_VERSAO"]
        lidas = [f"{expressao} AS {apelido}" for apelido, expressao in sorted(ctx["origens"].items())]
        de = ("(SELECT " + ", ".join(fixas + lidas) + " FROM origem_bruta)")
        onde = ""
    else:
        de = f"{CATALOGO}.{spec.get('origem_tabela', 'bronze.' + fonte.lower())}"
        onde = f"\nWHERE tipo_dado = '{tipo}'" if tipo and spec.get("origem_tabela") else ""
    return "SELECT\n  " + ",\n  ".join(select) + f"\nFROM {de}{onde}"


def processar(spark, modelo, fonte, tipo, mostrar=False):
    spec = modelo["fontes"][fonte]
    if spec.get("entidades") and not mostrar:
        montar_origem(spark, modelo, fonte, tipo or next(iter(spec["entidades"])))

    consulta = montar_select(modelo, fonte, tipo)
    if mostrar:
        print(consulta + ";\n")
        return

    spark.sql(consulta).createOrReplaceTempView("origem_canonica")

    # EVENTO primeiro; a extensao depois, porque depende do EVENTO_IDT.
    for tabela in ["EVENTO"] + ([spec["extensao"]] if spec.get("extensao") else []):
        colunas = list(modelo["entidades"][tabela]["campos"])
        chave = modelo["entidades"][tabela]["chave"][0]
        # A Silver e uma FUNCAO da Bronze: reprocessar a mesma origem tem de dar a
        # mesma linha. Por isso aqui e upsert, e nao so insercao — corrigir uma regra
        # do de/para e rodar de novo atualiza o que ja estava gravado. Quem nao pode
        # ser alterada e a Bronze, que guarda o que chegou.
        spark.sql(f"""
            MERGE INTO {CATALOGO}.silver.{tabela} destino
            USING origem_canonica origem ON destino.{chave} = origem.{chave}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"  {CATALOGO}.silver.{tabela}: {spark.table(f'{CATALOGO}.silver.{tabela}').count()} linhas")

    _relatar_dominios(spark, modelo, fonte, tipo)


def _relatar_dominios(spark, modelo, fonte, tipo):
    """A promessa do dominio controlado: valor que nao casa nao passa em silencio."""
    campos = modelo["entidades"]["EVENTO"]["campos"]
    mapeadas = {a.split(".")[-1]
                for _, mapa in _blocos_de_mapeamento(modelo["fontes"][fonte])
                for r in mapa.values() for a in _lista(r["campo"])}
    # so faz sentido cobrar dominio de coluna que ESTA fonte mapeia; as demais
    # sao nulas por nao terem origem, e nao por o valor nao ter casado.
    com_dominio = [c for c, s in campos.items()
                   if isinstance(s, dict) and s.get("dominio") and c in mapeadas]
    if not com_dominio:
        return
    contagens = ", ".join(f"sum(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}" for c in com_dominio)
    linha = spark.sql(f"SELECT {contagens} FROM origem_canonica").collect()[0].asDict()
    vazios = {c: n for c, n in linha.items() if n}
    if vazios:
        print(f"  atencao — valores que nao casaram com o dominio: {vazios}")


# =============================================================================

def get_spark():
    # importado aqui, e nao no topo, para que --listar e --mostrar-sql rodem
    # fora do container, sem Spark instalado
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("canonico_para_silver")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def listar(modelo):
    """Mostra o que o modelo declara e o que ja da para processar."""
    print(f"{len(modelo['entidades'])} tabelas, {len(modelo['dominios'])} dominios\n")
    for fonte, spec in modelo["fontes"].items():
        alvos = sorted({r["transformacao"] for _, m in _blocos_de_mapeamento(spec)
                        for r in m.values() if isinstance(r, dict) and "transformacao" in r})
        pendentes = [t for t in alvos
                     if t not in TRANSFORMACOES or TRANSFORMACOES[t].__name__ == "_f"]
        estado = "PRONTA" if not pendentes else f"aguarda {', '.join(pendentes)}"
        tipos = list(spec.get("tipos") or spec.get("entidades") or {})
        print(f"  {fonte:7s} {estado}")
        print(f"          tipos: {', '.join(tipos)}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ddl", action="store_true", help="cria as tabelas a partir do modelo")
    p.add_argument("--seeds", action="store_true", help="carrega as tabelas de referencia")
    p.add_argument("--fonte", help="fonte a processar (ex.: SIM)")
    p.add_argument("--tipo", help="tipo dentro da fonte (ex.: gps)")
    p.add_argument("--listar", action="store_true", help="mostra o que o modelo declara")
    p.add_argument("--mostrar-sql", action="store_true", help="imprime o SQL gerado e nao executa")
    p.add_argument("--modelo", help="caminho do modelo_canonico.yaml")
    a = p.parse_args()

    try:
        # so quem vai processar uma fonte precisa do de/para inteiro validado
        modelo = carregar_modelo(achar_modelo(a.modelo), validar_mapeamentos=bool(a.fonte))
    except ModeloInvalido as e:
        raise SystemExit(f"\n{e}\n")
    print(f"modelo canonico v{modelo['metadados']['versao']} carregado"
          + (" e validado" if a.fonte else ""))

    if a.listar:
        return listar(modelo)
    if not (a.ddl or a.seeds or a.fonte):
        p.error("escolha --ddl, --seeds, --fonte ou --listar")

    spark = None if a.mostrar_sql else get_spark()
    try:
        if a.ddl:
            criar_tabelas(spark, modelo, a.mostrar_sql)
        if a.seeds:
            carregar_seeds(spark, modelo)
        if a.fonte:
            processar(spark, modelo, a.fonte, a.tipo, a.mostrar_sql)
    except TransformacaoPendente as e:
        raise SystemExit(f"\n{e}\n")
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
