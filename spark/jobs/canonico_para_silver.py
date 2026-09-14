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
import math
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
    _resolver_heranca(modelo)
    if validar_mapeamentos:
        validar(modelo)
    # os seeds sao relativos ao proprio modelo, e nao a raiz do repositorio
    modelo["_dir"] = caminho.parent
    return modelo


def _resolver_heranca(modelo: dict) -> None:
    """`herda: <irma>` — a receita comeca como copia da irma e troca so o que declara.

    Nas secoes de mapeamento (sidecar, campos, extensao) a troca e campo a campo:
    o escaneado do RELPER herda as treze colunas da planilha e muda apenas a
    modalidade, o metodo e a leitura que produziu as celulas.
    """
    secoes = ("sidecar", "campos", "extensao")
    for spec in (modelo.get("fontes") or {}).values():
        blocos = spec.get("entidades") or {}
        for nome, bloco in list(blocos.items()):
            if not isinstance(bloco, dict) or not bloco.get("herda"):
                continue
            base = blocos[bloco["herda"]]
            novo = {**base, **{k: v for k, v in bloco.items() if k not in secoes}}
            for secao in secoes:
                if isinstance(base.get(secao), dict) or isinstance(bloco.get(secao), dict):
                    novo[secao] = {**(base.get(secao) or {}), **(bloco.get(secao) or {})}
            blocos[nome] = novo


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
      qualquer outro   -> CAMPOS['Ef Pres'], o cabecalho como esta na planilha
    """
    if regra.get("caminho"):
        return f"get_json_object({ctx['coluna_payload']}, '{regra['caminho']}')"
    if origem in ctx.get("sidecar", ()):
        return _registrar(ctx, "sc", origem, f"SIDECAR['{origem}']")
    if origem.startswith("_"):
        return "NULL"       # pseudo-campo: nao existe na origem
    if ctx.get("campos_em_mapa"):
        # `grafias` lista outros rotulos para a MESMA medida: a revisao do
        # formulario renomeou a coluna, mas o significado nao mudou. Vale o
        # primeiro que o registro tiver.
        nomes = [origem] + list(regra.get("grafias") or [])
        apelidos = [_registrar(ctx, "cmp", n, f"CAMPOS['{n}']") for n in nomes]
        valor = apelidos[0] if len(apelidos) == 1 else f"coalesce({', '.join(apelidos)})"

        # `compoe_com` junta varios campos num valor so: a chave natural da
        # operacao no C2_A chega partida em `operacao` ('Perseu') e `ano`
        # ('2024'), e o codigo canonico e a juncao — PERSEU_2024.
        for outro in (regra.get("compoe_com") or []):
            apelido = _registrar(ctx, "cmp", outro, f"CAMPOS['{outro}']")
            valor = f"concat_ws('_', {valor}, {apelido})"
        return valor
    return origem


def _registrar(ctx, prefixo, nome, expressao) -> str:
    """Guarda a leitura do mapa para virar coluna simples, e devolve o apelido.

    Por que nao usar CAMPOS['Ef Pres'] direto na expressao final: o Spark recusa
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
    """Texto ISO 8601 -> instante.

    `fuso: UTC` le a hora como UTC quando o texto nao traz fuso — sem isso o
    Spark usaria o fuso da sessao, que e configuracao da maquina e nao do dado.
    """
    if regra.get("fuso") == "UTC":
        return f"to_timestamp(concat(substring({origem}, 1, 16), '+00:00'), \"yyyy-MM-dd'T'HH:mmXXX\")"
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
    valor = f"upper(substring(trim({origem}), {posicao}, 1))"
    # O OCR le 'A2' como 'AZ'. Letra ou algarismo fora da escala vira vazio, e
    # nao sujeira: vale o dominio declarado para a coluna em EVENTO.
    dominio = ctx["modelo"]["entidades"]["EVENTO"]["campos"][alvo]["dominio"]
    validos = ", ".join(f"'{v}'" for v in ctx["modelo"]["dominios"][dominio]["valores"])
    return f"CASE WHEN length(trim({origem})) = 2 AND {valor} IN ({validos}) THEN {valor} END"


# Nome da coluna que a view `origem_bruta` ja traz, por tabela da Bronze referida.
_ELO_DA_BRONZE = {"ARQUIVO": "ARQUIVO_IDT", "RECEPCAO_BRUTA": "RECEPCAO_IDT",
                  "EXTRACAO": "EXTRACAO_IDT"}


def _comparavel(expressao: str) -> str:
    """Deixa dois textos comparaveis: sem acento, sem marca de ordinal e sem a
    letra que a acompanha. '1ª Cia', '1a Cia' e '1 Cia' viram todas '1 cia'."""
    sem_acento = f"translate(lower(trim({expressao})), 'ªº°áàâãéêíóôõúüç', 'aooaaaaeeiooouuc')"
    return f"regexp_replace(regexp_replace({sem_acento}, '([0-9]+)[ao]\\\\b', '$1'), ' +', ' ')"


def _simples(texto):
    """Minusculas, sem acento, espacos simples. O ordinal FICA ('1o' nao vira '1'):
    e justamente a letra que o OCR troca por zero, e a distancia precisa ve-la."""
    texto = unicodedata.normalize("NFKD", str(texto or "").strip().lower()).encode("ascii", "ignore").decode()
    return re.sub(" +", " ", texto)


def _distancia_edicao(a, b):
    """Distancia de Levenshtein: quantas letras trocar, inserir ou apagar para ir de a a b."""
    anterior = list(range(len(b) + 1))
    for i, x in enumerate(a, 1):
        atual = [i]
        for k, y in enumerate(b, 1):
            atual.append(min(anterior[k] + 1, atual[k - 1] + 1, anterior[k - 1] + (x != y)))
        anterior = atual
    return anterior[-1]


def _mais_parecido(lido, contexto, candidatos, limite):
    """O valor da lista FECHADA mais parecido com o que foi lido, ou None.

    `candidatos` e [(valor_a_devolver, texto_guardado)]. So concorrem os que terminam
    com o `contexto` (a OM remetente). A comparacao e com o INICIO do texto guardado,
    do tamanho do que foi lido: a celula escaneada vem truncada ('.../51o Esqd Ca').
    Acima do `limite`, ou empatado, devolve None — melhor vazio e visivel do que a
    unidade vizinha em silencio.
    """
    lido = _simples(lido)
    if not lido:
        return None
    contexto = _simples(contexto) if contexto else ""
    notas = sorted((_distancia_edicao(lido, _simples(texto)[:len(lido)]), valor)
                   for valor, texto in candidatos
                   if not contexto or _simples(texto).endswith(contexto))
    if not notas or notas[0][0] > limite:
        return None
    if len(notas) > 1 and notas[1][0] == notas[0][0]:
        return None
    return notas[0][1]


def _registrar_mais_parecido(spark, modelo, mapa):
    """Registra no Spark uma funcao por tabela de referencia usada com `busca: mais_parecido`.
    A tabela e lida UMA vez, aqui, e a lista viaja junto com a funcao para os executores."""
    from pyspark.sql.types import StringType

    for regra in mapa.values():
        if not isinstance(regra, dict) or regra.get("busca") != "mais_parecido":
            continue
        tabela = regra["referencia"]
        chave = modelo["entidades"][tabela]["chave"][0]
        casa = "UNIDADE_SIGLA" if tabela == "REF_UNIDADE" else chave
        candidatos = [(r[0], r[1]) for r in spark.table(f"{CATALOGO}.silver.{tabela}").select(chave, casa).collect()]
        limite = int(regra.get("limite", 2))
        spark.udf.register(f"mais_parecido_{tabela.lower()}",
                           lambda lido, contexto, _c=candidatos, _l=limite: _mais_parecido(lido, contexto, _c, _l),
                           StringType())


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

    # Referencia a uma tabela da BRONZE nao e consulta: o valor ja veio na view,
    # porque e um elo de linhagem (ou um metadado tecnico do proprio arquivo).
    if tabela in _ELO_DA_BRONZE:
        return coluna or _ELO_DA_BRONZE[tabela]

    # `busca: mais_parecido` — o texto lido e atribuido ao valor MAIS PARECIDO da
    # tabela (ver _mais_parecido). A escolha e feita em Python: a subconsulta
    # correlacionada do Spark nao aceita a coluna de fora dentro da agregacao que
    # escolheria o menor.
    if regra.get("busca") == "mais_parecido":
        contexto = (_registrar(ctx, "sc", regra["contexto"], f"SIDECAR['{regra['contexto']}']")
                    if regra.get("contexto") else "CAST(NULL AS STRING)")
        return f"mais_parecido_{tabela.lower()}({origem}, {contexto})"

    entidade = ctx["modelo"]["entidades"][tabela]
    chave = entidade["chave"][0]
    devolve = coluna or chave
    casa = "UNIDADE_SIGLA" if tabela == "REF_UNIDADE" else chave
    de = f"FROM {CATALOGO}.silver.{tabela} r"
    guardado, digitado = _comparavel(f"r.{casa}"), _comparavel(origem)

    # `contexto` nao e modo de busca: e um estreitamento. Restringe as linhas
    # candidatas ao que pertence a unidade que remeteu o arquivo.
    estreita = ""
    if regra.get("contexto"):
        contexto = _comparavel(_registrar(ctx, "sc", regra["contexto"], f"SIDECAR['{regra['contexto']}']"))
        estreita = (f" AND ({guardado} LIKE concat('%', {contexto}) OR {guardado} = {contexto})")

    busca = regra.get("busca", "igual")
    if busca == "comeca_por":
        # o que foi digitado e o COMECO do valor guardado: '1a Cia' -> '1a Cia Fuz/511o...'
        return f"(SELECT max(r.{devolve}) {de} WHERE {guardado} LIKE concat({digitado}, '%'){estreita})"
    if busca == "contido":
        # o valor guardado aparece DENTRO do que foi digitado. Varios podem caber
        # (a sigla do pelotao termina com a da companhia, que termina com a do
        # batalhao) — vale o MAIS LONGO, que e o mais especifico. O max sobre um
        # par (tamanho, valor) escolhe justamente esse.
        return (f"(SELECT max(named_struct('tamanho', length(r.{casa}), 'valor', r.{devolve})).valor "
                f"{de} WHERE {digitado} LIKE concat('%', {guardado}, '%'){estreita})")
    return f"(SELECT max(r.{devolve}) {de} WHERE {guardado} = {digitado}{estreita})"


def _t_gazetteer(origem, regra, ctx, alvo=None):
    """Referencia textual -> ponto. Nao converte: CONSULTA o indice de nomes."""
    filtro = ""
    if (regra.get("parametros") or {}).get("tipo"):
        filtro = f"AND g.LOCAL_TIPO_COD = '{regra['parametros']['tipo']}' "
    return (
        f"(SELECT max(g.GEOMETRIA_WKT) FROM {CATALOGO}.silver.REF_GAZETTEER g "
        f"WHERE lower(trim(g.REFERENCIA_TXT)) = lower(trim({origem})) {filtro})"
    )


def _t_celula(origem, regra, ctx, alvo=None):
    """Le uma celula pelo cabecalho decretado no formulario.

    Irma da `json`: a origem ja foi resolvida para CAMPOS['<cabecalho>'] — a
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
        # `celulas:` acrescenta celulas da propria linha — no FOGOS, a col_A (observador + hora)
        partes += [_registrar(ctx, "cmp", c, f"CAMPOS['{c}']") for c in regra.get("celulas", [])]
        return f"concat_ws('-', {', '.join(partes)})"
    if alvo == "FUNCAO_C2_INDIC":
        # S quando o dado nasceu DENTRO de um sistema de C2; N quando o documento
        # foi apenas remetido ao repositorio, como e o caso do formulario.
        return "'S'" if ctx["fonte"] in ("C2_A", "C2_B") else "'N'"
    if alvo == "FORMULARIO_VERSAO_COD":
        return "FORMULARIO_VERSAO"          # calculada ao abrir a grade
    if alvo == "CHEGADA_DATA":
        return "RECEBIMENTO_DATA"           # quando a Bronze recebeu o arquivo
    if alvo == "FUNCAO_COMBATE_COD":
        # a especie do desenho decide a funcao: um campo de minas e PROTECAO,
        # uma linha de fase e MOV_MANOBRA. O tipo ja foi resolvido pelo dominio.
        tipo = ctx["projecoes"].get("TIPO_COD", "NULL")
        return f"CASE WHEN {tipo} = 'OBSTACULO' THEN 'PROTECAO' ELSE 'MOV_MANOBRA' END"
    if alvo == "RELATO_TXT" and regra.get("celulas"):
        # varias celulas da linha num texto so, na ordem declarada — no FOGOS, col_G a col_L
        lidas = [_registrar(ctx, "cmp", c, f"CAMPOS['{c}']") for c in regra["celulas"]]
        return f"concat_ws(' | ', {', '.join(lidas)})"
    raise TransformacaoPendente(f"derivado sem regra implementada para {alvo}")


def _wkt_de_geojson(texto):
    """GeoJSON -> WKT, os tres desenhos que um sistema de C2 produz.

    Escrito em Python e registrado como funcao de SQL: a alternativa seria um
    CASE de tres ramos com manipulacao de listas aninhadas dentro do SQL, que
    ninguem consegue ler. Aqui a conversao cabe em dez linhas.

    GeoJSON poe [longitude, latitude]; WKT tambem, nessa ordem.
    """
    if not texto:
        return None
    import json as _json
    try:
        geometria = _json.loads(texto)
    except ValueError:
        return None
    tipo = str(geometria.get("type", "")).upper()
    coordenadas = geometria.get("coordinates")
    if not coordenadas:
        return None

    def par(p):
        return f"{p[0]} {p[1]}"

    if tipo == "POINT":
        corpo = par(coordenadas)
    elif tipo == "LINESTRING":
        corpo = ", ".join(par(p) for p in coordenadas)
    elif tipo == "POLYGON":
        corpo = ", ".join("(" + ", ".join(par(p) for p in anel) + ")" for anel in coordenadas)
    else:
        return None
    return f"{tipo}({corpo})"


def _ponto_de_dms(texto):
    """'22° 41\' 38.8" S, 45° 07\' 10.7" W' -> POINT(-45.119639 -22.694111).

    Grau-minuto-segundo e como um militar le uma carta e digita a coordenada.
    O hemisferio vem por letra: S e W (ou O, de Oeste) sao negativos. Texto que
    nao traz duas coordenadas — 'Sem localizacao', por exemplo — devolve vazio,
    e nao erro: relato sem coordenada e caso legitimo.
    """
    if not texto:
        return None
    import re
    partes = re.findall(r"(\d+)\D+(\d+)\D+([\d.]+)\D*([NSEWLO])", texto.upper())
    if len(partes) != 2:
        return None

    def decimal(grau, minuto, segundo, hemisferio):
        valor = int(grau) + int(minuto) / 60 + float(segundo) / 3600
        return -valor if hemisferio in ("S", "W", "O") else valor

    latitude, longitude = decimal(*partes[0]), decimal(*partes[1])
    return f"POINT({longitude:.6f} {latitude:.6f})"


def _t_dms_para_ponto(origem, regra, ctx, alvo=None):
    """Coordenada em grau-minuto-segundo, escrita por extenso, vira ponto."""
    return f"dms_para_ponto({origem})"


def _t_geojson_para_wkt(origem, regra, ctx, alvo=None):
    """Geometria em GeoJSON -> o mesmo desenho em WKT, o texto que o modelo usa."""
    return f"geojson_para_wkt({origem})"


# -----------------------------------------------------------------------------
# Coordenada decametrica (Anexo I do EB60-ME-12.301) -> ponto
# -----------------------------------------------------------------------------
# 'EEEEE-NNNNN' e o par UTM (Leste, Norte) em DEZENAS de metros, so com os cinco
# ultimos digitos. O Leste cabe inteiro; o Norte perde o digito dos milhares de
# quilometros ('49457' pode ser 7.494.570 m ou 6.494.570 m). Quem desempata e a AREA
# DE OPERACOES (REF_OPERACAO.AREA_WKT): ela da a zona UTM, e so uma das opcoes cai
# perto dela — a area tem dezenas de km, e as opcoes distam 1.000 km entre si.

_WGS84_A, _WGS84_F, _UTM_K0 = 6378137.0, 1 / 298.257223563, 0.9996


def _utm_para_latlon(leste, norte, zona, sul):
    """UTM -> (lat, lon) em graus, WGS84. Formulas classicas (Snyder, 1987); erro submetrico."""
    e2 = _WGS84_F * (2 - _WGS84_F)
    ep2 = e2 / (1 - e2)
    x, y = leste - 500000.0, (norte - 10_000_000.0) if sul else norte
    mu = y / _UTM_K0 / (_WGS84_A * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = (mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
            + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
            + (151 * e1 ** 3 / 96) * math.sin(6 * mu) + (1097 * e1 ** 4 / 512) * math.sin(8 * mu))
    n1 = _WGS84_A / math.sqrt(1 - e2 * math.sin(phi1) ** 2)
    t1, c1 = math.tan(phi1) ** 2, ep2 * math.cos(phi1) ** 2
    r1 = _WGS84_A * (1 - e2) / (1 - e2 * math.sin(phi1) ** 2) ** 1.5
    d = x / (n1 * _UTM_K0)
    lat = phi1 - (n1 * math.tan(phi1) / r1) * (
        d ** 2 / 2 - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
        + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720)
    lon = (d - (1 + 2 * t1 + c1) * d ** 3 / 6
           + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120) / math.cos(phi1)
    return math.degrees(lat), (zona - 1) * 6 - 180 + 3 + math.degrees(lon)


def _ponto_de_decametrica(texto, area_wkt, alcance_km=150):
    """'50231-49457' + area de operacoes -> 'POINT(lon lat)', ou None.

    O ponto sai no CENTRO da quadricula de 10 m que o documento escreveu. Se nenhuma
    opcao do Norte cair a menos de `alcance_km` do centro da area, devolve None: um
    digito que o OCR leu muito errado vira vazio visivel, e nao um bombardeio do
    outro lado do mapa.
    """
    lido = re.search(r"(\d{5})\s*-\s*(\d{5})", str(texto or ""))
    vertices = [(float(lon), float(lat)) for lon, lat in
                re.findall(r"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)", str(area_wkt or ""))]
    if not lido or not vertices:
        return None
    if len(vertices) > 1 and vertices[0] == vertices[-1]:
        vertices = vertices[:-1]           # o poligono WKT repete o primeiro vertice no fim
    lon_c = sum(v[0] for v in vertices) / len(vertices)
    lat_c = sum(v[1] for v in vertices) / len(vertices)
    zona, sul = int((lon_c + 180) // 6) + 1, lat_c < 0
    leste = int(lido.group(1)) * 10 + 5
    melhor = None
    for milhares in range(10):             # o digito perdido do Norte
        lat, lon = _utm_para_latlon(leste, int(lido.group(2)) * 10 + 5 + milhares * 1_000_000, zona, sul)
        km = 111.195 * math.hypot(lat - lat_c, (lon - lon_c) * math.cos(math.radians(lat_c)))
        if melhor is None or km < melhor[0]:
            melhor = (km, lat, lon)
    if melhor[0] > alcance_km:
        return None
    return f"POINT({melhor[2]:.6f} {melhor[1]:.6f})"


def _t_decametrica_para_ponto(origem, regra, ctx, alvo=None):
    """'50231-49457' -> ponto WKT, ancorado na area da operacao do proprio registro."""
    operacao = _registrar(ctx, "sc", "operacao", "SIDECAR['operacao']")
    return f"decametrica_para_ponto({origem}, {operacao})"


def _registrar_decametrica(spark, mapa):
    """Registra a conversao decametrica, com as areas de REF_OPERACAO lidas uma vez."""
    from pyspark.sql.types import StringType

    if not any(isinstance(r, dict) and r.get("transformacao") == "decametrica_para_ponto" for r in mapa.values()):
        return
    areas = {r[0]: r[1] for r in spark.table(f"{CATALOGO}.silver.REF_OPERACAO").select("OPERACAO_COD", "AREA_WKT").collect()}
    spark.udf.register("decametrica_para_ponto",
                       lambda texto, operacao, _a=areas: _ponto_de_decametrica(texto, _a.get(operacao)),
                       StringType())


_MESES_GDH = {m: i for i, m in enumerate(
    ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"], start=1)}
_MESES_GDH.update({"FEB": 2, "APR": 4, "MAY": 5, "AUG": 8, "SEP": 9, "OCT": 10, "DEC": 12})


def _instante_de_gdh(texto, fuso="+00:00"):
    """Grupo data-hora militar -> texto ISO com fuso, ou None.

    '250539 NOV 24' = dia 25, 05h39, novembro de 2024. So vale quando a celula tem
    EXATAMENTE UM grupo data-hora: o OCR ora poe o codinome antes, ora depois, e as
    vezes junta o grupo da linha vizinha — com dois, qualquer escolha seria palpite.
    """
    achados = [a for a in re.findall(r"\b(\d{2})(\d{2})(\d{2})\s*([A-Z]{3})\s*(\d{2})\b", str(texto or "").upper())
               if a[3] in _MESES_GDH]
    if len(achados) != 1:
        return None
    dia, hora, minuto, mes, ano = achados[0]
    if not (1 <= int(dia) <= 31 and int(hora) < 24 and int(minuto) < 60):
        return None
    return f"{2000 + int(ano)}-{_MESES_GDH[mes]:02d}-{dia}T{hora}:{minuto}:00{fuso}"


def _t_gdh(origem, regra, ctx, alvo=None):
    return f"CAST(gdh_para_instante({origem}, '{regra.get('fuso', '+00:00')}') AS TIMESTAMP)"


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
    "geojson_para_wkt": _t_geojson_para_wkt,
    "dms_para_ponto":  _t_dms_para_ponto,
    "decametrica_para_ponto": _t_decametrica_para_ponto,
    "gdh":             _t_gdh,
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

# O esquema da view de origem, declarado e nao adivinhado: numa planilha as
# colunas de captura sao SEMPRE nulas, e o Spark nao consegue inferir o tipo de
# uma coluna que so tem nulo. A ordem aqui e a ordem das tuplas de _abrir_grade.
def _esquema_origem():
    from pyspark.sql.types import (MapType, StringType, StructField, StructType, TimestampType)
    texto, mapa = StringType(), MapType(StringType(), StringType())
    return StructType([
        StructField("ARQUIVO_IDT", texto), StructField("RECEPCAO_IDT", texto),
        StructField("EXTRACAO_IDT", texto), StructField("RECEBIMENTO_DATA", TimestampType()),
        StructField("CAPTURA_GEOMETRIA_WKT", texto), StructField("CAPTURA_DATA", TimestampType()),
        StructField("SIDECAR", mapa), StructField("CAMPOS", mapa),
        StructField("SOBRA_JSON", texto), StructField("FORMULARIO_VERSAO", texto),
        StructField("EXTRACAO_MODELO", texto),
    ])


def _abrir_grade(registro, cabecalhos, principais, grafias=None, por_posicao=False, rotulos=None):
    """Uma linha de EXTRACAO (uma planilha) -> N linhas, uma por observacao.

    A linha de cabecalho e PROCURADA, nunca fixada: e a primeira que contem pelo
    menos metade dos cabecalhos decretados. Acima dela ficam titulo, operacao,
    OM e data — que sao do documento, nao das observacoes.

    Com `por_posicao` (o formulario escaneado), o rotulo lido NAO nomeia a coluna:
    o OCR le 'Ef Prev' como 'Erev' e junta dois rotulos numa celula so. A linha de
    cabecalho e achada por SEMELHANCA, e as colunas recebem os nomes decretados na
    ORDEM do formulario; o que passar do numero decretado vai para a sobra com o
    rotulo lido. Grade com MENOS colunas que o decretado nao e aberta: por posicao,
    todos os valores dali em diante cairiam na coluna errada.
    """
    import difflib
    import json as _json
    import re as _re
    import unicodedata as _ud

    def _limpo(texto):
        texto = _ud.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode().lower()
        return "".join(ch for ch in texto if ch.isalnum())

    def _palavras(texto):
        texto = _ud.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode().lower()
        return {p for p in _re.findall(r"[a-z0-9]+", texto) if len(p) > 1}

    def _parecido(lido, nome):
        # a maior de duas medidas: letras na ordem ('Erev' ~ 'Ef Prev') e palavras
        # do rotulo presentes em qualquer ordem ('Bombardeada (5) Area F' ~ 'Area Bombardeada')
        letras = difflib.SequenceMatcher(None, _limpo(lido), _limpo(nome)).ratio()
        alvo = _palavras(nome)
        return max(letras, len(alvo & _palavras(lido)) / len(alvo) if alvo else 0)

    celulas_por_aba = _json.loads(registro["SAIDA_TXT"])["abas"]
    sidecar = _json.loads(registro["SIDECAR_JSON"])
    declarados, decretados = set(cabecalhos), set(principais)
    grafias = grafias or {}
    saida = []

    for grade in celulas_por_aba.values():
        titulos, primeira, lidos = None, 0, []
        for i, linha in enumerate(grade):
            if por_posicao:
                casados = sum(1 for c in linha if str(c or "").strip()
                              and max(_parecido(c, n) for n in (rotulos or declarados)) >= 0.75)
                achou = casados >= len(principais) / 2
            else:
                presentes = {str(c).strip() for c in linha if c is not None}
                achou = len(presentes & declarados) >= len(declarados) / 2
            if achou:
                lidos, primeira = [str(c).strip() if c is not None else "" for c in linha], i + 1
                if not por_posicao:
                    titulos = lidos
                elif len(lidos) >= len(principais):
                    titulos = list(principais) + [lidos[j] or f"coluna_{j + 1}"
                                                  for j in range(len(principais), len(lidos))]
                break
        if titulos is None:
            continue                      # aba sem o formulario, ou escaneado com coluna faltando

        if por_posicao:
            # a coluna que a revisao renomeou diz a versao: o rotulo lido parece
            # mais com a grafia alternativa ('Comb %') do que com a original
            versao = "V2" if any(_parecido(lidos[j], g) > _parecido(lidos[j], nome)
                                 for j, nome in enumerate(principais) for g in grafias.get(nome, [])) else "V1"
        else:
            # V1 quando a planilha traz todos os cabecalhos decretados; V2 quando
            # falta algum, porque a revisao do formulario renomeou colunas. So os
            # rotulos PRINCIPAIS contam: a grafia alternativa e justamente o sinal de
            # que a planilha e da outra versao. Coluna a mais nao muda a versao — vai
            # para a sobra.
            versao = "V1" if decretados <= set(titulos) else "V2"

        for linha in grade[primeira:]:
            # celula vazia e vazia: a planilha a devolve como None, o Docling como ''
            valores = {t: v for t, v in zip(titulos, linha) if t and v is not None and str(v).strip() != ""}
            if not valores:
                continue                  # linha em branco: fim da tabela
            # tupla na ordem de _esquema_origem()
            saida.append((
                registro["ARQUIVO_IDT"], registro["RECEPCAO_IDT"], registro["EXTRACAO_IDT"],
                registro["RECEBIMENTO_DATA"],
                registro["CAPTURA_GEOMETRIA_WKT"], registro["CAPTURA_DATA"],
                {k: (None if v is None else str(v)) for k, v in sidecar.items()},
                {k: str(v) for k, v in valores.items() if k in declarados},
                _sobra_como_json(valores, declarados),
                versao,
                registro["EXTRACAO_MODELO"],
            ))
    return saida


def _sobra_como_json(valores: dict, declarados: set):
    """O que a OM acrescentou por conta propria. Vazio e None, nao '{}': a sobra
    tem de ser a excecao visivel, e nao uma coluna preenchida em toda linha."""
    import json as _json
    sobra = {c: v for c, v in valores.items() if c not in declarados}
    return _json.dumps(sobra, ensure_ascii=False) if sobra else None


# As duas formas devolvem a MESMA view, `origem_bruta`, com as mesmas colunas:
#   ARQUIVO_IDT · RECEPCAO_IDT · EXTRACAO_IDT · RECEBIMENTO_DATA   os elos de linhagem
#   CAMPOS       mapa nome -> valor: e daqui que o de/para le todo campo
#   SIDECAR      mapa nome -> valor do .json que acompanha o binario
#   SOBRA_JSON · FORMULARIO_VERSAO    so as fontes que chegam em formulario usam
# Dai para a frente o de/para e igual para as seis fontes.

def montar_origem(spark, modelo, fonte, tipo):
    """Cria a view `origem_bruta`, com uma linha por observacao.

    Duas formas, conforme o `conteudo:` declarado no modelo:
      payload   o registro ja vem com os campos nomeados (o JSON do C2_A, do
                relato e do incidente). Uma linha da Bronze e uma observacao.
      extracao  os campos vem do que foi lido do binario, numa grade que
                precisa ser aberta — e um arquivo vira N observacoes.
    """
    bloco = modelo["fontes"][fonte]["entidades"][tipo]
    if bloco["conteudo"] == "payload":
        return _origem_do_payload(spark, fonte, tipo)
    if bloco["conteudo"] == "extracao_campos":
        return _origem_dos_campos_extraidos(spark, fonte)
    return _origem_da_extracao(spark, modelo, fonte, tipo, bloco)


def _origem_dos_campos_extraidos(spark, fonte):
    """A extracao ja devolveu campos NOMEADOS — nao ha grade a abrir.

    E o caso da cadeia de dois modelos: o transcritor devolveu texto corrido, e o
    modelo de linguagem leu esse texto e montou {codinome, referencia_local,
    texto}. Uma linha por interpretacao; o elo com a transcricao que a originou
    fica em EXTRACAO_ORIGEM_IDT, e e por ele que se reconhece qual extracao e a
    do modelo de linguagem.

    Aos campos que o modelo devolveu junta-se `texto_lido`: o que ele LEU, quando
    a leitura anterior foi um OCR. No informe do INTEL as paginas do OCR sao o
    corpo do documento, e o modelo nao as repete na resposta. Na voz fica vazio.
    """
    spark.sql(f"""
        SELECT a.ARQUIVO_IDT, r.RECEPCAO_IDT, i.EXTRACAO_IDT, r.RECEBIMENTO_DATA,
               a.CAPTURA_GEOMETRIA_WKT, a.CAPTURA_DATA,
               from_json(r.CONTEUDO_JSON_TXT, 'map<string,string>') AS SIDECAR,
               map_concat(from_json(i.SAIDA_TXT, 'map<string,string>'),
                          map('texto_lido', array_join(
                              from_json(o.SAIDA_TXT, 'struct<paginas:array<string>>').paginas, '\\n'))) AS CAMPOS,
               CAST(NULL AS STRING) AS SOBRA_JSON,
               CAST(NULL AS STRING) AS FORMULARIO_VERSAO,
               -- a cadeia de IA, da ultima leitura para a primeira: o modelo de
               -- linguagem <- o transcritor ou o OCR que ele leu
               nullif(concat_ws(' <- ',
                   CASE WHEN i.INFERENCIA_INDIC = 'S' THEN concat_ws(' ', i.FERRAMENTA_NOME, i.MODELO_NOME) END,
                   CASE WHEN o.INFERENCIA_INDIC = 'S' THEN concat_ws(' ', o.FERRAMENTA_NOME, o.MODELO_NOME) END), '') AS EXTRACAO_MODELO
        FROM {CATALOGO}.bronze.EXTRACAO i
        JOIN {CATALOGO}.bronze.EXTRACAO o ON o.EXTRACAO_IDT = i.EXTRACAO_ORIGEM_IDT
        JOIN {CATALOGO}.bronze.ARQUIVO a ON a.ARQUIVO_IDT = i.ARQUIVO_IDT
        JOIN {CATALOGO}.bronze.RECEPCAO_BRUTA r ON r.ARQUIVO_IDT = a.ARQUIVO_IDT
        WHERE i.EXTRACAO_ORIGEM_IDT IS NOT NULL AND i.STATUS_COD = 'OK'
          AND r.SISTEMA_ORIGEM_COD = '{fonte}'
    """).createOrReplaceTempView("origem_bruta")
    total = spark.table("origem_bruta").count()
    if not total:
        raise SystemExit(f"\nnenhuma interpretacao de {fonte} na Bronze — rode a extracao antes\n")
    print(f"  {total} interpretacoes -> {total} observacoes")


def _origem_do_payload(spark, fonte, tipo):
    """O registro estruturado vira mapa nome -> valor, sem mais nada.

    Nao ha o que abrir: cada linha de RECEPCAO_BRUTA ja e uma observacao — a
    ingestao desdobrou o lote JSON registro a registro. Campo aninhado (como a
    geometria do C2_A) vem como o proprio texto JSON, que e o que a conversao
    de geometria espera receber.

    A juncao com ARQUIVO traz o metadado tecnico do binario quando ha um: a
    coordenada do EXIF da foto, que e fonte INDEPENDENTE do que o operador
    digitou — e por isso a que vale para o incidente.

    Como se separam as receitas da mesma fonte: pela PASTA em que o arquivo
    pousou. A convencao da landing e landing/<operacao>/<fonte>/<receita>/, e
    RECEPCAO_BRUTA guarda esse endereco em ORIGEM_URI_TXT.
    """
    spark.sql(f"""
        SELECT r.ARQUIVO_IDT, r.RECEPCAO_IDT,
               r.RECEBIMENTO_DATA,
               map_concat(from_json(r.CONTEUDO_JSON_TXT, 'map<string,string>'),
                          coalesce(from_json(i.SAIDA_TXT, 'map<string,string>'), map())) AS CAMPOS,
               map() AS SIDECAR,
               CAST(NULL AS STRING) AS SOBRA_JSON,
               CAST(NULL AS STRING) AS FORMULARIO_VERSAO,
               a.CAPTURA_GEOMETRIA_WKT, a.CAPTURA_DATA,
               i.EXTRACAO_IDT,
               CASE WHEN i.INFERENCIA_INDIC = 'S' THEN concat_ws(' ', i.FERRAMENTA_NOME, i.MODELO_NOME) END AS EXTRACAO_MODELO
        FROM {CATALOGO}.bronze.RECEPCAO_BRUTA r
        LEFT JOIN {CATALOGO}.bronze.ARQUIVO a ON a.ARQUIVO_IDT = r.ARQUIVO_IDT
        -- a interpretacao do modelo de linguagem sobre ESTE registro, se houve.
        -- Os campos dela entram no mesmo mapa que os do registro: para o de/para
        -- nao ha diferenca entre um campo que veio escrito e um que foi inferido
        -- — a diferenca fica registrada aqui, em EXTRACAO, que e onde ela importa.
        LEFT JOIN {CATALOGO}.bronze.EXTRACAO i
               ON i.RECEPCAO_IDT = r.RECEPCAO_IDT AND i.STATUS_COD = 'OK'
        WHERE r.SISTEMA_ORIGEM_COD = '{fonte}'
          AND r.ORIGEM_URI_TXT LIKE '%/{tipo}/%'
    """).createOrReplaceTempView("origem_bruta")
    total = spark.table("origem_bruta").count()
    if not total:
        raise SystemExit(f"\nnenhum registro de {fonte}/{tipo} na Bronze — rode a DAG de ingestao antes\n")
    print(f"  {total} registros na Bronze -> {total} observacoes")


def _origem_da_extracao(spark, modelo, fonte, tipo, bloco):
    """A grade de celulas que a extracao gravou vira uma linha por observacao.

    Junta as tres tabelas da Bronze que contam a historia de um arquivo:
    o que foi LIDO dele (EXTRACAO), o arquivo em si (ARQUIVO) e o que veio
    ESCRITO ao lado dele (RECEPCAO_BRUTA, o sidecar).
    """
    do_sidecar = bloco.get("sidecar") if isinstance(bloco.get("sidecar"), dict) else {}
    mapa = _mapa_do_bloco(bloco)
    principais = [c for c in mapa if not c.startswith("_") and c not in do_sidecar]
    # as grafias alternativas tambem sao cabecalhos declarados: sem isso elas
    # cairiam na sobra, como se a OM tivesse inventado a coluna. Mas nao entram
    # no conjunto que identifica a VERSAO do formulario — ver _abrir_grade.
    cabecalhos = principais + [g for c in principais for g in (mapa[c].get("grafias") or [])]
    grafias = {c: list(mapa[c].get("grafias") or []) for c in principais}
    por_posicao = bloco.get("cabecalho") == "posicao"
    # `colunas:` declara o formulario pela ORDEM das colunas, com um rotulo que so
    # serve para achar a linha de cabecalho — o Relatorio de Bombardeio: col_A a col_L
    rotulos = None
    if isinstance(bloco.get("colunas"), dict):
        principais = cabecalhos = list(bloco["colunas"])
        rotulos, grafias = list(bloco["colunas"].values()), {}

    # `cede_a`: a mesma remessa que chegou tambem pela receita irma (sidecar de
    # texto identico) fica so com a irma. O escaneado do RELPER cede a planilha:
    # leitura direta vale mais que inferencia, e as duas juntas contariam a mesma
    # situacao duas vezes.
    sem_gemeo = ""
    if bloco.get("cede_a"):
        modalidade_irma = modelo["fontes"][fonte]["entidades"][bloco["cede_a"]]["modalidade"]
        sem_gemeo = (f"AND NOT EXISTS (SELECT 1 FROM {CATALOGO}.bronze.RECEPCAO_BRUTA r2 "
                     f"WHERE r2.SISTEMA_ORIGEM_COD = '{fonte}' AND r2.MODALIDADE_COD = '{modalidade_irma}' "
                     f"AND r2.CONTEUDO_JSON_TXT = r.CONTEUDO_JSON_TXT)")

    # So entra extracao que E grade (tem `abas`): o mesmo PDF tem tambem a leitura
    # do tesseract, que e texto corrido e nao tem o que abrir.
    grade = spark.sql(f"""
        SELECT e.EXTRACAO_IDT, e.SAIDA_TXT, a.ARQUIVO_IDT,
               r.RECEPCAO_IDT, r.CONTEUDO_JSON_TXT AS SIDECAR_JSON, r.RECEBIMENTO_DATA,
               a.CAPTURA_GEOMETRIA_WKT, a.CAPTURA_DATA,
               CASE WHEN e.INFERENCIA_INDIC = 'S' THEN concat_ws(' ', e.FERRAMENTA_NOME, e.MODELO_NOME) END AS EXTRACAO_MODELO
        FROM {CATALOGO}.bronze.EXTRACAO e
        JOIN {CATALOGO}.bronze.ARQUIVO a ON a.ARQUIVO_IDT = e.ARQUIVO_IDT
        JOIN {CATALOGO}.bronze.RECEPCAO_BRUTA r ON r.ARQUIVO_IDT = a.ARQUIVO_IDT
        WHERE a.MODALIDADE_COD = '{bloco["modalidade"]}'
          AND r.SISTEMA_ORIGEM_COD = '{fonte}'
          AND e.STATUS_COD = 'OK'
          AND get_json_object(e.SAIDA_TXT, '$.abas') IS NOT NULL
          {sem_gemeo}
    """)
    if grade.rdd.isEmpty():
        raise SystemExit(f"\nnenhuma extracao de {fonte}/{tipo} na Bronze — rode a DAG de extracao antes\n")

    linhas = grade.rdd.flatMap(lambda r: _abrir_grade(r, cabecalhos, principais, grafias, por_posicao, rotulos))
    spark.createDataFrame(linhas, _esquema_origem()).createOrReplaceTempView("origem_bruta")
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
    da_bronze_v3 = bool(blocos)          # le da view origem_bruta, com o mapa CAMPOS

    if da_bronze_v3:
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

    ctx = {"modelo": modelo, "fonte": fonte, "sidecar": sidecar,
           "campos_em_mapa": da_bronze_v3, "origens": {},
           "coluna_payload": spec.get("coluna_payload", "payload")}
    # de qual celula sai a unidade da linha — o derivado de REGISTRO_ORIGEM_COD precisa
    unidade = next((o for o, r in mapa.items()
                    if "UNIDADE_REPORTANTE_COD" in _lista(r["campo"]) and not o.startswith("_")), None)
    ctx["coluna_unidade"] = _registrar(ctx, "cmp", unidade, f"CAMPOS['{unidade}']") if unidade else None

    # `projecoes` no ctx porque um derivado pode depender de outra coluna ja
    # resolvida — a funcao de combate do MCC sai do tipo, por exemplo. Por isso
    # os derivados sao resolvidos numa segunda passada.
    projecoes = ctx["projecoes"] = {}
    derivados = []
    for origem, regra in mapa.items():
        entrada = _expressao_origem(origem, regra, ctx)
        for alvo in _lista(regra["campo"]):
            coluna = alvo.split(".")[-1]
            if regra["transformacao"] == "derivado":
                derivados.append((entrada, regra, coluna))
                continue
            projecoes[coluna] = TRANSFORMACOES[regra["transformacao"]](entrada, regra, ctx, coluna)
    for entrada, regra, coluna in derivados:
        projecoes[coluna] = _t_derivado(entrada, regra, ctx, coluna)

    # A cadeia de IA por tras do evento vem do job, e nao da receita: nenhuma fonte com
    # inferencia no caminho pode deixar de avisar quem le o dado que ele deve conferir.
    if da_bronze_v3:
        projecoes.setdefault("EXTRACAO_MODELO_NOME", "EXTRACAO_MODELO")

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

    if da_bronze_v3:
        # camada de baixo: cada campo lido de um mapa vira coluna simples
        fixas = ["ARQUIVO_IDT", "RECEPCAO_IDT", "EXTRACAO_IDT", "RECEBIMENTO_DATA",
                 "SOBRA_JSON", "FORMULARIO_VERSAO", "CAPTURA_GEOMETRIA_WKT", "CAPTURA_DATA", "EXTRACAO_MODELO"]
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
        from pyspark.sql.types import StringType
        spark.udf.register("geojson_para_wkt", _wkt_de_geojson, StringType())
        spark.udf.register("dms_para_ponto", _ponto_de_dms, StringType())
        spark.udf.register("gdh_para_instante", _instante_de_gdh, StringType())
        receita = tipo or next(iter(spec["entidades"]))
        mapa_receita = _mapa_do_bloco(spec["entidades"][receita])
        _registrar_mais_parecido(spark, modelo, mapa_receita)
        _registrar_decametrica(spark, mapa_receita)
        montar_origem(spark, modelo, fonte, receita)

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
    blocos = modelo["fontes"][fonte].get("entidades") or {}
    mapa = _mapa_do_bloco(blocos[tipo]) if tipo in blocos else {}
    mapeadas = {a.split(".")[-1] for r in mapa.values() for a in _lista(r["campo"])}
    # so faz sentido cobrar dominio de coluna que ESTA RECEITA mapeia. As demais
    # sao nulas por nao terem origem — o TIPO_COD do relato, por exemplo, esta
    # em `extraidos_por_llm` e so sera preenchido quando a DAG de IA existir.
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
        # O leitor VETORIZADO do Iceberg (Arrow, memoria fora do heap da JVM)
        # derruba o executor sem excecao Java — codigo 134 — ao reler a tabela
        # durante um MERGE que atualiza linhas. Desligado: a leitura fica um
        # pouco mais lenta e nao quebra. Diagnosticado em 12/09/2026.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
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
