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

def carregar_modelo(caminho: Path) -> dict:
    modelo = yaml.safe_load(caminho.read_text(encoding="utf-8"))
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


def _blocos_de_mapeamento(spec: dict):
    """Devolve (nome_do_bloco, mapa_de_campos) para todas as formas que uma fonte assume."""
    if spec.get("comum"):
        yield "comum", spec["comum"]
    for nome, mapa in (spec.get("tipos") or {}).items():
        yield nome, mapa
    for nome, ent in (spec.get("entidades") or {}).items():
        yield nome, ent.get("campos") or {}


# =============================================================================
# 2. TRANSFORMACOES — o unico lugar do projeto com codigo por conversao
# =============================================================================
# Cada funcao recebe o nome da coluna de origem e a regra declarada no modelo, e
# devolve uma EXPRESSAO SQL. `ctx` traz o modelo inteiro, para as transformacoes
# que precisam consultar dominios.

def _expressao_origem(origem, regra, ctx):
    """De onde o valor vem, ANTES de qualquer conversao.

    As transformacoes compoem: primeiro resolve-se a origem (coluna, caminho
    dentro do JSON, ou pseudo-campo), e so depois a conversao e aplicada por
    cima. Sem isso, `dominio` sobre um campo que mora dentro do payload seria
    aplicado a uma coluna que nao existe.
    """
    if regra.get("caminho"):
        return f"get_json_object({ctx['coluna_payload']}, '{regra['caminho']}')"
    if origem.startswith("_"):
        return "NULL"       # pseudo-campo: nao existe na origem
    return origem


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


def _t_referencia(origem, regra, ctx, alvo=None):
    """Consulta a uma tabela de referencia. Subconsulta escalar: continua sendo expressao."""
    alvo = regra["referencia"]
    tabela, coluna = (alvo.split(".", 1) + [None])[:2] if "." in alvo else (alvo, None)
    entidade = ctx["modelo"]["entidades"][tabela]
    chave = entidade["chave"][0]
    devolve = coluna or chave
    casa = "UNIDADE_SIGLA" if tabela == "REF_UNIDADE" else chave
    return (
        f"(SELECT r.{devolve} FROM {CATALOGO}.silver.{tabela} r "
        f"WHERE lower(trim(r.{casa})) = lower(trim({origem})) LIMIT 1)"
    )


def _t_gazetteer(origem, regra, ctx, alvo=None):
    """Referencia textual -> ponto. Nao converte: CONSULTA o indice de nomes."""
    filtro = ""
    if (regra.get("parametros") or {}).get("tipo"):
        filtro = f"AND g.LOCAL_TIPO_COD = '{regra['parametros']['tipo']}' "
    return (
        f"(SELECT g.GEOMETRIA_WKT FROM {CATALOGO}.silver.REF_GAZETTEER g "
        f"WHERE lower(trim(g.REFERENCIA_TEXTO)) = lower(trim({origem})) {filtro}LIMIT 1)"
    )


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
        faltando = set(colunas) - set(registros[0])
        if faltando:
            raise ModeloInvalido(f"{caminho.name}: faltam colunas {faltando}")
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
# 5. O DE/PARA VIRA UM SELECT
# =============================================================================

def montar_select(modelo, fonte, tipo=None):
    """Monta a consulta que le a origem e devolve colunas canonicas."""
    spec = modelo["fontes"][fonte]
    ctx = {"modelo": modelo, "coluna_payload": spec.get("coluna_payload", "payload")}

    mapa = dict(spec.get("comum") or {})
    if tipo:
        mapa.update((spec.get("tipos") or {})[tipo])
    else:
        entidades = spec.get("entidades") or {}
        if len(entidades) != 1:
            raise ModeloInvalido(f"{fonte}: informe --tipo (opcoes: {list(entidades)})")
        mapa.update(next(iter(entidades.values()))["campos"])

    projecoes = {}
    for origem, regra in mapa.items():
        entrada = _expressao_origem(origem, regra, ctx)
        for alvo in _lista(regra["campo"]):
            coluna = alvo.split(".")[-1]
            projecoes[coluna] = TRANSFORMACOES[regra["transformacao"]](
                entrada, regra, ctx, coluna
            )

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

    colunas_evento = list(modelo["entidades"]["EVENTO"]["campos"])
    select = ",\n  ".join(
        f"{projecoes[c]} AS {c}" if c in projecoes else f"CAST(NULL AS {_TIPOS[modelo['entidades']['EVENTO']['campos'][c]['tipo']]}) AS {c}"
        for c in colunas_evento
    )
    origem_tabela = spec.get("origem_tabela", f"bronze.{fonte.lower()}")
    onde = f"\nWHERE tipo_dado = '{tipo}'" if tipo and spec.get("origem_tabela") else ""
    return f"SELECT\n  {select}\nFROM {CATALOGO}.{origem_tabela}{onde}"


def processar(spark, modelo, fonte, tipo, mostrar=False):
    consulta = montar_select(modelo, fonte, tipo)
    if mostrar:
        print(consulta + ";\n")
        return

    spark.sql(consulta).createOrReplaceTempView("origem_canonica")
    colunas = list(modelo["entidades"]["EVENTO"]["campos"])
    spark.sql(f"""
        MERGE INTO {CATALOGO}.silver.EVENTO destino
        USING origem_canonica origem ON destino.EVENTO_IDT = origem.EVENTO_IDT
        WHEN NOT MATCHED THEN INSERT ({", ".join(colunas)})
            VALUES ({", ".join("origem." + c for c in colunas)})
    """)
    _relatar_dominios(spark, modelo, fonte, tipo)
    print(f"  {fonte}" + (f"/{tipo}" if tipo else "") + ": MERGE concluido")


def _relatar_dominios(spark, modelo, fonte, tipo):
    """A promessa do dominio controlado: valor que nao casa nao passa em silencio."""
    campos = modelo["entidades"]["EVENTO"]["campos"]
    com_dominio = [c for c, s in campos.items() if isinstance(s, dict) and s.get("dominio")]
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
        pendentes = [t for t in alvos if TRANSFORMACOES[t].__name__ == "_f"]
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
        modelo = carregar_modelo(achar_modelo(a.modelo))
    except ModeloInvalido as e:
        raise SystemExit(f"\n{e}\n")
    print(f"modelo canonico v{modelo['metadados']['versao']} carregado e validado")

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
