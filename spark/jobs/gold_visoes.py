"""
gold_visoes.py — Le a Silver e reescreve uma visao da Gold.

    python3 gold_visoes.py --visao pitcic
    python3 gold_visoes.py --visao pitcic --mostrar-sql

Cada visao e UMA tabela, declarada em `entidades` do modelo canonico (camada gold)
e criada pelo --ddl do canonico_para_silver.py, como todas. Dentro dela, os
elementos do processo doutrinario ficam empilhados pelas fases: a coluna FASE_NRO diz
a fase, ETAPA_COD a etapa. Os parametros que mudam o resultado (raio da corroboracao,
siglas do MD33-M-02) estao em `visoes:` do modelo, e nao aqui.

A tabela e reescrita inteira a cada execucao (INSERT OVERWRITE). A Gold e funcao
da Silver: rodar de novo nunca duplica, e a versao anterior fica no historico do
Iceberg.

As contas geograficas sao funcoes Python deste arquivo, registradas como UDF: o
Spark nao traz funcoes geograficas, e uma biblioteca so para isso seria mais uma
instalacao em duas imagens. Ficam sem cache (lru_cache): o executor recebe a
funcao serializada e nao consegue remontar o cache do driver.
"""

import argparse
import math
import re
import time

from canonico_para_silver import CATALOGO, _TIPOS, achar_modelo, carregar_modelo

SILVER = f"{CATALOGO}.silver"
GOLD = f"{CATALOGO}.gold"


# =============================================================================
# Contas geograficas (viram UDF)
# =============================================================================

_NUMERO = re.compile(r"-?\d+(?:\.\d+)?")


def _centro(wkt):
    """(lon, lat) do ponto, ou a media dos vertices de uma linha ou area."""
    n = [float(x) for x in _NUMERO.findall(wkt or "")]
    pares = list(zip(n[0::2], n[1::2]))
    if len(pares) > 2 and pares[0] == pares[-1]:
        pares = pares[:-1]                     # o anel da area repete o primeiro vertice
    if not pares:
        return None
    return sum(p[0] for p in pares) / len(pares), sum(p[1] for p in pares) / len(pares)


def distancia_m(a_wkt, b_wkt):
    """Distancia na superficie da Terra (haversine), em metros, entre os centros."""
    a, b = _centro(a_wkt), _centro(b_wkt)
    if a is None or b is None:
        return None
    f1, f2 = math.radians(a[1]), math.radians(b[1])
    h = (math.sin((f2 - f1) / 2) ** 2
         + math.cos(f1) * math.cos(f2) * math.sin(math.radians(b[0] - a[0]) / 2) ** 2)
    return 2 * 6371008.8 * math.asin(math.sqrt(h))


# =============================================================================
# Pecas de SQL
# =============================================================================

def _texto(valor):
    return "'" + str(valor).replace("'", "''") + "'"


def _sigla(expressao, siglas):
    """O valor exibido: a sigla do MD33-M-02 declarada em visoes.siglas, ou o proprio valor."""
    ramos = " ".join(f"WHEN {_texto(k)} THEN {_texto(v)}" for k, v in siglas.items())
    return f"CASE {expressao} {ramos} ELSE {expressao} END"


def _lista(expressao_array, siglas=None):
    """Lista de valores distintos, em ordem, separados por virgula; vazia vira nulo."""
    item = _sigla("t", siglas) if siglas else "t"
    return f"nullif(array_join(transform(array_sort({expressao_array}), t -> {item}), ', '), '')"


def _bloco(colunas, origem, **valores):
    """Um SELECT com TODAS as colunas da visao, na ordem da tabela.

    Cada etapa preenche so as colunas que fazem sentido para ela; as demais saem
    nulas. O CAST garante que os blocos se empilhem com o mesmo tipo.
    """
    valores.setdefault("ATUALIZACAO_DATA", "current_timestamp()")
    proj = ", ".join(f"CAST({valores.get(c, 'NULL')} AS {tipo}) AS {c}" for c, tipo in colunas)
    return f"SELECT {proj} FROM ({origem})"


# Descricao curta de um evento. O informe do INTEL guarda o documento inteiro lido
# pelo OCR, com cabecalho: o que interessa e o paragrafo 1.
_DESCRICAO = (r"coalesce(CASE WHEN SISTEMA_ORIGEM_COD = 'INTEL' THEN nullif(regexp_replace("
              r"regexp_extract(RELATO_TXT, '(?s)\\n\\s*1\\.\\s*(.+?)\\n\\s*2\\.', 1), '\\s+', ' '), '') END, "
              r"substring(regexp_replace(RELATO_TXT, '\\s+', ' '), 1, 300))")

# Armamento inimigo na primeira celula do relato do FOGOS: '2/Ob/Me' (quantidade/tipo/calibre).
_ARMA = r"regexp_extract(RELATO_TXT, '^\\s*(\\d+)\\s*/\\s*([A-Za-z]+)\\s*/\\s*([A-Za-z]+)', {})"


# =============================================================================
# PITCIC — EB70-MC-10.336
# =============================================================================

def montar_pitcic(modelo):
    """Devolve (visoes_de_apoio, consulta_final). Nenhuma regra de parametro mora aqui."""
    v, sg = modelo["visoes"]["PITCIC"], modelo["visoes"]["siglas"]
    colunas = [(c, _TIPOS[s["tipo"]]) for c, s in modelo["entidades"][v["tabela"]]["campos"].items()]
    raio, janela = v["corroboracao"]["raio_metro"], v["corroboracao"]["janela_segundos"]
    tipos_lugar = ", ".join(_texto(t) for t in v["lugar_referencia_tipos"])
    tipos_ameaca = ", ".join(_texto(t) for t in v["ameaca_tipos"])
    restricao = "|".join(v["restricao_movimento_palavras"]).replace("\\", "\\\\")
    c = lambda nome: _texto(sg.get(nome, nome))          # constante ja em sigla
    periodo = "CASE " + " ".join(
        f"WHEN hour(OCORRENCIA_DATA) < {p['ate_hora']} THEN {c(p['nome'])}" for p in v["periodos"]) + " END"

    apoio = [
        ("op", f"SELECT max(OPERACAO_COD) AS OPERACAO_COD FROM {SILVER}.REF_OPERACAO"),
        ("lugar", f"SELECT LOCAL_NOME, LOCAL_TIPO_COD, GEOMETRIA_WKT FROM {SILVER}.REF_GAZETTEER "
                  f"WHERE LOCAL_TIPO_COD IN ({tipos_lugar})"),
        ("ev", f"SELECT e.*, m.MEDIDA_ESPECIE_COD, m.MEDIDA_NOME, m.OBSERVACAO FROM {SILVER}.EVENTO e "
               f"LEFT JOIN {SILVER}.MEDIDA_COORDENACAO m ON m.EVENTO_IDT = e.EVENTO_IDT"),
        # o lugar de referencia mais proximo de cada evento que o PITCIC usa
        ("perto", f"""
            SELECT EVENTO_IDT, LOCAL_NOME, LOCAL_TIPO_COD, DIST FROM (
                SELECT EVENTO_IDT, LOCAL_NOME, LOCAL_TIPO_COD, DIST,
                       row_number() OVER (PARTITION BY EVENTO_IDT ORDER BY DIST) AS ORDEM
                FROM (SELECT e.EVENTO_IDT, l.LOCAL_NOME, l.LOCAL_TIPO_COD,
                             distancia_m(e.GEOMETRIA_WKT, l.GEOMETRIA_WKT) AS DIST
                      FROM ev e CROSS JOIN lugar l
                      WHERE e.GEOMETRIA_WKT IS NOT NULL
                        AND e.TIPO_COD IN ({tipos_ameaca}, 'AVISTAMENTO', 'OBSTACULO', 'INCIDENTE')))
            WHERE ORDEM = 1"""),
        ("ameaca", f"SELECT * FROM ev WHERE TIPO_COD IN ({tipos_ameaca})"),
        # corroboracao: evento da ameaca de OUTRA fonte, perto no espaco e no tempo
        ("corrobora", f"""
            SELECT a.EVENTO_IDT, collect_set(b.SISTEMA_ORIGEM_COD) AS SISTEMAS,
                   collect_set(b.MODALIDADE_ORIGEM_COD) AS MODALIDADES, collect_list(b.EVENTO_IDT) AS OUTROS
            FROM ameaca a JOIN ameaca b
              ON b.SISTEMA_ORIGEM_COD <> a.SISTEMA_ORIGEM_COD
             AND abs(unix_timestamp(b.OCORRENCIA_DATA) - unix_timestamp(a.OCORRENCIA_DATA)) <= {janela}
            WHERE distancia_m(a.GEOMETRIA_WKT, b.GEOMETRIA_WKT) <= {raio}
            GROUP BY a.EVENTO_IDT"""),
    ]

    evento_simples = dict(OPERACAO_COD="OPERACAO_COD", GEOMETRIA_WKT="GEOMETRIA_WKT", INICIO_DATA="OCORRENCIA_DATA",
                          UNIDADE_COD="UNIDADE_REPORTANTE_COD", TIPO_TXT=_sigla("TIPO_COD", sg),
                          EXTRACAO_MODELO_NOME="EXTRACAO_MODELO_NOME", EVENTO_IDT="array(EVENTO_IDT)")
    com_lugar = dict(LOCAL_REFERENCIA_NOME="LN", LOCAL_REFERENCIA_TIPO_COD=_sigla("LT", sg), LOCAL_DISTANCIA_METRO="round(DIST)",
                     EVENTO_QNT="1", FONTE_QNT="1", MODALIDADE_TXT="MODALIDADE_ORIGEM_COD")
    junta_lugar = "LEFT JOIN perto p ON p.EVENTO_IDT = e.EVENTO_IDT"
    campos_lugar = "p.LOCAL_NOME AS LN, p.LOCAL_TIPO_COD AS LT, p.DIST"

    blocos = [
        # --- fase 1: definicao do ambiente operacional ------------------------------
        _bloco(colunas, "SELECT l.*, op.OPERACAO_COD AS OP FROM lugar l CROSS JOIN op "
                        "WHERE l.LOCAL_TIPO_COD IN ('PONTO_NOTAVEL', 'LOCALIDADE')",
               FASE_NRO="1", ETAPA_COD=c("CARACTERISTICAS"), ELEMENTO_TIPO_COD=c("LUGAR"), ELEMENTO_NOME="LOCAL_NOME",
               ELEMENTO_DCRI=_sigla("LOCAL_TIPO_COD", sg), OPERACAO_COD="OP", GEOMETRIA_WKT="GEOMETRIA_WKT"),
        _bloco(colunas, f"SELECT * FROM {SILVER}.REF_OPERACAO",
               FASE_NRO="1", ETAPA_COD=c("CARACTERISTICAS"), ELEMENTO_TIPO_COD=c("AREA_OPERACAO"),
               ELEMENTO_NOME="OPERACAO_NOME", OPERACAO_COD="OPERACAO_COD", GEOMETRIA_WKT="AREA_WKT",
               INICIO_DATA="INICIO_DATA", FIM_DATA="FIM_DATA", UNIDADE_COD="UNIDADE_RESP_COD"),
        _bloco(colunas, "SELECT * FROM ev WHERE MEDIDA_ESPECIE_COD = 'LIMITE'",
               FASE_NRO="1", ETAPA_COD=c("ZONA_ACAO"), ELEMENTO_TIPO_COD=c("LIMITE"), ELEMENTO_NOME="MEDIDA_NOME",
               **evento_simples),

        # --- fase 2: efeitos do ambiente sobre as operacoes -------------------------
        # consideracoes civis: cada localidade com os eventos a ate N metros
        _bloco(colunas, f"""
            SELECT l.LOCAL_NOME, l.GEOMETRIA_WKT, max(op.OPERACAO_COD) AS OP,
                   count(e.EVENTO_IDT) AS N, count(DISTINCT e.SISTEMA_ORIGEM_COD) AS NF,
                   collect_set(e.MODALIDADE_ORIGEM_COD) AS MODS, collect_set(e.TIPO_COD) AS TIPOS,
                   min(e.OCORRENCIA_DATA) AS INI, max(e.OCORRENCIA_DATA) AS FIM, collect_list(e.EVENTO_IDT) AS IDS
            FROM lugar l CROSS JOIN op
            LEFT JOIN (SELECT l2.LOCAL_NOME AS LN, e2.*
                       FROM lugar l2 CROSS JOIN ev e2
                       WHERE l2.LOCAL_TIPO_COD = 'LOCALIDADE' AND e2.GEOMETRIA_WKT IS NOT NULL
                         AND e2.TIPO_COD NOT IN ('POSICAO', 'SITUACAO_UNIDADE', 'MCC')
                         AND distancia_m(l2.GEOMETRIA_WKT, e2.GEOMETRIA_WKT) <= {v['localidade_raio_metro']}) e
              ON e.LN = l.LOCAL_NOME
            WHERE l.LOCAL_TIPO_COD = 'LOCALIDADE'
            GROUP BY l.LOCAL_NOME, l.GEOMETRIA_WKT""",
               FASE_NRO="2", ETAPA_COD=c("CONSIDERACOES_CIVIS"), ELEMENTO_TIPO_COD=c("LOCALIDADE"), ELEMENTO_NOME="LOCAL_NOME",
               OPERACAO_COD="OP", GEOMETRIA_WKT="GEOMETRIA_WKT", INICIO_DATA="INI", FIM_DATA="FIM",
               TIPO_TXT=_lista("TIPOS", sg), EVENTO_QNT="N", FONTE_QNT="NF", MODALIDADE_TXT=_lista("MODS"),
               EVENTO_IDT="IDS"),
        # consideracoes civis: o que foi visto e NAO e forca oponente
        _bloco(colunas, f"SELECT e.*, {campos_lugar} FROM ev e {junta_lugar} WHERE e.TIPO_COD = 'AVISTAMENTO'",
               FASE_NRO="2", ETAPA_COD=c("CONSIDERACOES_CIVIS"), ELEMENTO_TIPO_COD=c("AVISTAMENTO"), ELEMENTO_DCRI=_DESCRICAO,
               FONTE_CONFIABILIDADE_COD="FONTE_CONFIABILIDADE_COD", INFO_CREDIBILIDADE_COD="INFO_CREDIBILIDADE_COD",
               **evento_simples, **com_lugar),
        # vias de acesso: a rodovia vira linha, ligando os marcos de km em ordem
        _bloco(colunas, r"""
            SELECT regexp_extract(LOCAL_NOME, '^(.*) km', 1) AS VIA, max(op.OPERACAO_COD) AS OP,
                   concat('LINESTRING(', array_join(transform(array_sort(collect_list(named_struct(
                       'km', CAST(regexp_extract(LOCAL_NOME, 'km (\\d+)', 1) AS INT),
                       'xy', regexp_extract(GEOMETRIA_WKT, 'POINT\\((.*)\\)', 1)))), s -> s.xy), ', '), ')') AS WKT
            FROM lugar CROSS JOIN op WHERE LOCAL_TIPO_COD = 'RODOVIA_KM' GROUP BY 1""",
               FASE_NRO="2", ETAPA_COD=c("VIAS_ACESSO"), ELEMENTO_TIPO_COD=c("RODOVIA"), ELEMENTO_NOME="VIA",
               OPERACAO_COD="OP", GEOMETRIA_WKT="WKT"),
        _bloco(colunas, "SELECT * FROM ev WHERE MEDIDA_ESPECIE_COD = 'EIXO_PROGRESSAO'",
               FASE_NRO="2", ETAPA_COD=c("VIAS_ACESSO"), ELEMENTO_TIPO_COD=c("EIXO_PROGRESSAO"), ELEMENTO_NOME="MEDIDA_NOME",
               **evento_simples),
        # restricoes ao movimento: obstaculos e incidentes que param a via
        _bloco(colunas, f"""
            SELECT e.*, {campos_lugar} FROM ev e {junta_lugar}
            WHERE e.TIPO_COD = 'OBSTACULO'
               OR (e.TIPO_COD = 'INCIDENTE' AND lower(e.RELATO_TXT) RLIKE '{restricao}')""",
               FASE_NRO="2", ETAPA_COD=c("RESTRICOES_MOVIMENTO"), ELEMENTO_TIPO_COD=_sigla("TIPO_COD", sg),
               ELEMENTO_NOME=f"coalesce(MEDIDA_NOME, {_sigla('MEDIDA_ESPECIE_COD', sg)})",
               ELEMENTO_DCRI=f"coalesce(OBSERVACAO, {_DESCRICAO})", **evento_simples, **com_lugar),

        # --- fase 3: avaliacao da ameaca (DiCoVAP) ----------------------------------
        _bloco(colunas, f"""
            SELECT a.*, p.LOCAL_NOME AS LN, p.LOCAL_TIPO_COD AS LT, p.DIST,
                   1 + size(coalesce(c.OUTROS, array())) AS N,
                   1 + size(coalesce(c.SISTEMAS, array())) AS NF,
                   array_distinct(concat(array(a.MODALIDADE_ORIGEM_COD), coalesce(c.MODALIDADES, array()))) AS MODS,
                   concat(array(a.EVENTO_IDT), coalesce(c.OUTROS, array())) AS IDS,
                   CASE WHEN a.SISTEMA_ORIGEM_COD = 'FOGOS' THEN {_ARMA.format(1)} END AS AQ,
                   CASE WHEN a.SISTEMA_ORIGEM_COD = 'FOGOS' THEN {_ARMA.format(2)} END AS AT,
                   CASE WHEN a.SISTEMA_ORIGEM_COD = 'FOGOS' THEN {_ARMA.format(3)} END AS AC
            FROM ameaca a
            LEFT JOIN perto p ON p.EVENTO_IDT = a.EVENTO_IDT
            LEFT JOIN corrobora c ON c.EVENTO_IDT = a.EVENTO_IDT""",
               FASE_NRO="3", ETAPA_COD=c("DICOVAP"), ELEMENTO_TIPO_COD=c("AMEACA"), ELEMENTO_DCRI=_DESCRICAO,
               OPERACAO_COD="OPERACAO_COD", GEOMETRIA_WKT="GEOMETRIA_WKT", LOCAL_REFERENCIA_NOME="LN",
               LOCAL_REFERENCIA_TIPO_COD=_sigla("LT", sg), LOCAL_DISTANCIA_METRO="round(DIST)", INICIO_DATA="OCORRENCIA_DATA",
               UNIDADE_COD="UNIDADE_REPORTANTE_COD", TIPO_TXT=_sigla("TIPO_COD", sg), EVENTO_QNT="N", FONTE_QNT="NF",
               MODALIDADE_TXT=_lista("MODS"), FONTE_CONFIABILIDADE_COD="FONTE_CONFIABILIDADE_COD",
               INFO_CREDIBILIDADE_COD="INFO_CREDIBILIDADE_COD", ARMA_QNT="nullif(AQ, '')",
               ARMA_TIPO_COD="nullif(AT, '')", ARMA_CALIBRE_COD="nullif(AC, '')",
               EXTRACAO_MODELO_NOME="EXTRACAO_MODELO_NOME", EVENTO_IDT="IDS"),

        # --- fase 4: materia-prima da matriz de eventos -----------------------------
        # atividade da ameaca por lugar de referencia e periodo do dia; as RIPI sao do EM
        _bloco(colunas, f"""
            SELECT LOCAL_NOME, LOCAL_TIPO_COD, DIA, PERIODO, max(WKT) AS WKT, max(OPERACAO_COD) AS OP,
                   count(*) AS N, count(DISTINCT SISTEMA_ORIGEM_COD) AS NF,
                   collect_set(MODALIDADE_ORIGEM_COD) AS MODS, collect_set(TIPO_COD) AS TIPOS,
                   min(OCORRENCIA_DATA) AS INI, max(OCORRENCIA_DATA) AS FIM, collect_list(EVENTO_IDT) AS IDS,
                   max(EXTRACAO_MODELO_NOME) AS IA
            FROM (SELECT a.*, p.LOCAL_NOME, p.LOCAL_TIPO_COD, l.GEOMETRIA_WKT AS WKT,
                         date_format(a.OCORRENCIA_DATA, 'dd/MM/yyyy') AS DIA, {periodo} AS PERIODO
                  FROM ameaca a JOIN perto p ON p.EVENTO_IDT = a.EVENTO_IDT
                  JOIN lugar l ON l.LOCAL_NOME = p.LOCAL_NOME)
            GROUP BY LOCAL_NOME, LOCAL_TIPO_COD, DIA, PERIODO""",
               FASE_NRO="4", ETAPA_COD=c("ATIVIDADE"), ELEMENTO_TIPO_COD=c("ATIVIDADE"),
               ELEMENTO_NOME="concat(LOCAL_NOME, ' | ', DIA, ' ', PERIODO)", OPERACAO_COD="OP", GEOMETRIA_WKT="WKT",
               LOCAL_REFERENCIA_NOME="LOCAL_NOME", LOCAL_REFERENCIA_TIPO_COD=_sigla("LOCAL_TIPO_COD", sg), INICIO_DATA="INI",
               FIM_DATA="FIM", TIPO_TXT=_lista("TIPOS", sg), EVENTO_QNT="N", FONTE_QNT="NF",
               MODALIDADE_TXT=_lista("MODS"), EXTRACAO_MODELO_NOME="IA", EVENTO_IDT="IDS"),
    ]
    return apoio, "\nUNION ALL\n".join(f"({b})" for b in blocos)


# =============================================================================
# PROBLEMA_DADOS — o que chegou errado e o que nao chegou
# =============================================================================

def montar_problema_dados(modelo):
    """Devolve (visoes_de_apoio, consulta_final).

    Quatro blocos, um por especie de problema, do mais visivel ao menos:

        CHEGOU_QUEBRADO        o arquivo chegou e nao pode sequer ser aberto
        SEM_SIDECAR            o binario chegou sem o .json que o descreve
        SIDECAR_SEM_ARQUIVO    o .json chegou e o binario nao
        NAO_CHEGOU             a OM nao remeteu um turno que a cadencia previa
        INCOMPLETO_NO_ARQUIVO  a remessa chegou faltando subunidade

    Os tres primeiros se veem so olhando a Bronze. Os dois ultimos NAO se veem no
    dado: precisam da expectativa declarada — a cadencia em visoes.PROBLEMA_DADOS
    e a ordem de batalha em REF_UNIDADE. E a diferenca entre "falta informacao" e
    "falta o RELPER da 2a Cia, turno da tarde de 27/11".

    O mesmo SQL esta em scripts/testes_qualidade.py, como teste. La ele avisa;
    aqui ele detalha.
    """
    v = modelo["visoes"]["PROBLEMA_DADOS"]
    colunas = [(c, _TIPOS[s["tipo"]]) for c, s in modelo["entidades"][v["tabela"]]["campos"].items()]
    com_binario = ", ".join(_texto(f) for f in v["fontes_com_binario"])
    turnos = ", ".join(_texto(t) for t in v["relper_turnos"])
    regra = lambda nome: _texto(nome)                # o teste de qualidade que acha o mesmo

    apoio = [
        # uma linha por (OM, dia, turno, subunidade) que EFETIVAMENTE chegou
        ("remessa", f"""
            SELECT u.SUPERIOR_COD AS OM_COD, to_date(e.OCORRENCIA_DATA) AS DIA, s.TURNO_COD,
                   e.UNIDADE_REPORTANTE_COD AS UNIDADE_COD, max(e.OPERACAO_COD) AS OPERACAO_COD
            FROM {SILVER}.EVENTO e
            JOIN {SILVER}.SITUACAO_UNIDADE s ON s.EVENTO_IDT = e.EVENTO_IDT
            JOIN {SILVER}.REF_UNIDADE u ON u.UNIDADE_COD = e.UNIDADE_REPORTANTE_COD
            GROUP BY 1, 2, 3, 4"""),
        ("turno_chegou", "SELECT OM_COD, DIA, TURNO_COD, max(OPERACAO_COD) AS OPERACAO_COD "
                         "FROM remessa GROUP BY 1, 2, 3"),
        ("op", "SELECT max(OPERACAO_COD) AS OPERACAO_COD FROM remessa"),
        # a janela e a OBSERVADA, da primeira a ultima remessa: ver visoes.PROBLEMA_DADOS
        ("dia", "SELECT explode(sequence(min(DIA), max(DIA), interval 1 day)) AS DIA FROM remessa"),
        # o que DEVERIA ter chegado: cada OM, em cada dia, nos turnos da cadencia
        ("esperado", f"""
            SELECT o.OM_COD, d.DIA, t.TURNO_COD
            FROM (SELECT DISTINCT OM_COD FROM remessa) o
            CROSS JOIN dia d
            CROSS JOIN (SELECT explode(array({turnos})) AS TURNO_COD) t"""),
        # quem preenche uma linha do RELPER: as SU da OM; se a OM nao tem SU, as fracoes diretas
        ("unidade_esperada", f"""
            SELECT f.SUPERIOR_COD AS OM_COD, f.UNIDADE_COD
            FROM {SILVER}.REF_UNIDADE f
            JOIN {SILVER}.REF_UNIDADE o ON o.UNIDADE_COD = f.SUPERIOR_COD AND o.ESCALAO_COD = 'OM'
            WHERE f.ESCALAO_COD = 'SU'
               OR f.SUPERIOR_COD NOT IN (SELECT DISTINCT SUPERIOR_COD FROM {SILVER}.REF_UNIDADE
                                         WHERE ESCALAO_COD = 'SU' AND SUPERIOR_COD IS NOT NULL)"""),
    ]

    blocos = [
        # 1. binario sem sidecar. A fonte fica vazia: quem a declarava era o sidecar
        # que se perdeu. O endereco em OBJETO_TXT ainda mostra de que pasta veio.
        _bloco(colunas, f"""
            SELECT a.ARQUIVO_URI_TXT, a.ARQUIVO_IDT, a.MODALIDADE_COD, a.OPERACAO_COD
            FROM {CATALOGO}.bronze.ARQUIVO a
            LEFT JOIN {CATALOGO}.bronze.RECEPCAO_BRUTA r ON r.ARQUIVO_IDT = a.ARQUIVO_IDT
            WHERE r.RECEPCAO_IDT IS NULL""",
               PROBLEMA_TIPO_COD=_texto("SEM_SIDECAR"), OPERACAO_COD="OPERACAO_COD",
               MODALIDADE_COD="MODALIDADE_COD", OBJETO_TXT="ARQUIVO_URI_TXT",
               OBJETO_IDT="ARQUIVO_IDT", DETECCAO_REGRA_TXT=regra("arquivo_tem_sidecar")),

        # 2. sidecar sem binario. So vale para as fontes que SEMPRE trazem arquivo:
        # no C2_A e no relato do C2_B, ARQUIVO_IDT vazio e o normal.
        _bloco(colunas, f"""
            SELECT ORIGEM_URI_TXT, RECEPCAO_IDT, SISTEMA_ORIGEM_COD, MODALIDADE_COD, OPERACAO_COD
            FROM {CATALOGO}.bronze.RECEPCAO_BRUTA
            WHERE SISTEMA_ORIGEM_COD IN ({com_binario}) AND ARQUIVO_IDT IS NULL""",
               PROBLEMA_TIPO_COD=_texto("SIDECAR_SEM_ARQUIVO"), OPERACAO_COD="OPERACAO_COD",
               SISTEMA_ORIGEM_COD="SISTEMA_ORIGEM_COD", MODALIDADE_COD="MODALIDADE_COD",
               OBJETO_TXT="ORIGEM_URI_TXT", OBJETO_IDT="RECEPCAO_IDT",
               DETECCAO_REGRA_TXT=regra("sidecar_achou_seu_binario")),

        # 3. a OM nao remeteu o turno. Nao ha arquivo a nomear: quem identifica o
        # que falta e a unidade, o dia e o turno.
        _bloco(colunas, """
            SELECT e.OM_COD, e.DIA, e.TURNO_COD, op.OPERACAO_COD
            FROM esperado e
            CROSS JOIN op
            LEFT JOIN turno_chegou c
              ON c.OM_COD = e.OM_COD AND c.DIA = e.DIA AND c.TURNO_COD = e.TURNO_COD
            WHERE c.OM_COD IS NULL""",
               PROBLEMA_TIPO_COD=_texto("NAO_CHEGOU"), OPERACAO_COD="OPERACAO_COD",
               SISTEMA_ORIGEM_COD=_texto("RELPER"), MODALIDADE_COD=_texto("PLANILHA"),
               UNIDADE_COD="OM_COD", REFERENCIA_DATA="DIA", TURNO_COD="TURNO_COD",
               DETECCAO_REGRA_TXT=regra("relper_turno_remetido")),

        # 4. a remessa chegou, mas sem todas as subunidades da OM. Aqui a unidade
        # que falta e a SUBUNIDADE, e nao a OM: e ela que o EM vai cobrar.
        _bloco(colunas, """
            SELECT t.OM_COD, t.DIA, t.TURNO_COD, x.UNIDADE_COD, t.OPERACAO_COD
            FROM turno_chegou t
            JOIN unidade_esperada x ON x.OM_COD = t.OM_COD
            LEFT JOIN remessa r
              ON r.OM_COD = t.OM_COD AND r.DIA = t.DIA AND r.TURNO_COD = t.TURNO_COD
             AND r.UNIDADE_COD = x.UNIDADE_COD
            WHERE r.UNIDADE_COD IS NULL""",
               PROBLEMA_TIPO_COD=_texto("INCOMPLETO_NO_ARQUIVO"), OPERACAO_COD="OPERACAO_COD",
               SISTEMA_ORIGEM_COD=_texto("RELPER"), MODALIDADE_COD=_texto("PLANILHA"),
               UNIDADE_COD="UNIDADE_COD", REFERENCIA_DATA="DIA", TURNO_COD="TURNO_COD",
               DETECCAO_REGRA_TXT=regra("relper_todas_subunidades")),

        # 5. o arquivo chegou e nao abriu. So entra o que NAO foi reenviado
        # corrigido depois: se o mesmo endereco ja esta em ARQUIVO ou em
        # RECEPCAO_BRUTA, o problema foi resolvido e sai da visao sozinho —
        # embora a linha de REJEICAO continue la, porque a Bronze e append-only.
        # O motivo e a mensagem do erro ficam em REJEICAO, alcancavel pelo
        # OBJETO_IDT: a visao aponta, a tabela detalha.
        _bloco(colunas, f"""
            SELECT r.ORIGEM_URI_TXT, r.REJEICAO_IDT
            FROM {CATALOGO}.bronze.REJEICAO r
            LEFT JOIN {CATALOGO}.bronze.RECEPCAO_BRUTA rb ON rb.ORIGEM_URI_TXT = r.ORIGEM_URI_TXT
            LEFT JOIN {CATALOGO}.bronze.ARQUIVO a ON a.ARQUIVO_URI_TXT = r.ORIGEM_URI_TXT
            WHERE rb.RECEPCAO_IDT IS NULL AND a.ARQUIVO_IDT IS NULL""",
               PROBLEMA_TIPO_COD=_texto("CHEGOU_QUEBRADO"), OBJETO_TXT="ORIGEM_URI_TXT",
               OBJETO_IDT="REJEICAO_IDT", DETECCAO_REGRA_TXT=regra("rejeitado_foi_reenviado")),
    ]
    return apoio, "\nUNION ALL\n".join(f"({b})" for b in blocos)


VISOES = {"pitcic": montar_pitcic, "problema_dados": montar_problema_dados}


# =============================================================================
# Execucao
# =============================================================================

def get_spark(fuso):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("gold_visoes")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.vectorization.enabled", "false")   # ver canonico_para_silver.get_spark
        .config("spark.sql.session.timeZone", fuso)                    # dia e periodo no horario local
        .getOrCreate()
    )


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--visao", required=True, choices=sorted(VISOES))
    p.add_argument("--mostrar-sql", action="store_true", help="imprime o SQL gerado e nao executa")
    p.add_argument("--modelo", help="caminho do modelo_canonico.yaml")
    a = p.parse_args()

    modelo = carregar_modelo(achar_modelo(a.modelo), validar_mapeamentos=False)
    spec = modelo["visoes"][a.visao.upper()]
    apoio, consulta = VISOES[a.visao](modelo)
    if a.mostrar_sql:
        for nome, sql in apoio:
            print(f"-- {nome}\n{sql};\n")
        print(consulta + ";")
        return

    inicio = time.time()
    spark = get_spark(modelo["visoes"]["fuso"])
    try:
        from pyspark.sql.types import DoubleType
        tabela = f"{GOLD}.{spec['tabela']}"
        if not spark.catalog.tableExists(tabela):
            raise SystemExit(f"\n{tabela} nao existe: rode canonico_para_silver.py --ddl\n")
        spark.udf.register("distancia_m", distancia_m, DoubleType())
        for nome, sql in apoio:
            spark.sql(sql).createOrReplaceTempView(nome)
            spark.catalog.cacheTable(nome)
        # SELECT * FROM (...): sem ele, o parentese logo apos o nome da tabela seria
        # lido como lista de colunas
        spark.sql(f"INSERT OVERWRITE {tabela} SELECT * FROM (\n{consulta}\n)")

        print(f"\n  {tabela}: {spark.table(tabela).count()} linhas em {time.time() - inicio:.0f} s")
        # o que o resumo agrupa e soma e declarado na visao, nao fixado aqui
        grupo = ", ".join(spec["resumo"])
        soma = spec.get("resumo_soma")
        spark.sql(f"SELECT {grupo}, count(*) AS linhas"
                  + (f", sum({soma}) AS {soma.lower()}" if soma else "")
                  + f" FROM {tabela} GROUP BY {grupo} ORDER BY {grupo}").show(50, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
