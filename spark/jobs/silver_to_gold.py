"""
Silver → Gold — Spark Job
Quatro visões analíticas alinhadas aos processos doutrinários do C2:
  gold.coc       — Cenário Operacional Comum (EB70-MC-10.205)
  gold.pitcic    — Integração Terreno/Met./Inimigo/Civis (EB70-MC-10.336)
  gold.ppcot     — Planejamento e Condução das Op. Terrestres (EB70-MC-10.211)
  gold.avaliacao — Avaliação e Monitoramento da Condução (EB70-MC-10.211 Cap.V)
"""
import argparse

from pyspark.sql import SparkSession


def _comentar(spark: SparkSession, tabela: str, comentarios: dict):
    for coluna, texto in comentarios.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{texto}'")


def _comentar_tabela(spark: SparkSession, tabela: str, descricao: str):
    spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES ('comment' = '{descricao}')")


def get_spark():
    return (
        SparkSession.builder
        .appName("silver_to_gold")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def criar_schema_gold(spark: SparkSession):
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.gold")


# ---------------------------------------------------------------------------
# Visão 1 — COC (Cenário Operacional Comum)
# Situação integrada por subunidade: JOIN cruzado entre todas as Funções de
# Combate. É o produto da gestão do conhecimento previsto no EB70-MC-10.205.
# ---------------------------------------------------------------------------
def gold_coc(spark: SparkSession):
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.coc")
    spark.sql("""
        CREATE TABLE lakehouse.gold.coc AS
        WITH ultima_posicao AS (
            SELECT batalhao_origem, subunidade,
                   latitude, longitude, velocidade, direcao,
                   timestamp_geracao AS ts_gps
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.gps
            ) WHERE rn = 1
        ),
        ultimo_pessoal AS (
            SELECT batalhao_origem, subunidade,
                   situacao_operacional,
                   efetivo_organico, efetivo_presente,
                   baixas_combate, baixas_nao_combate, evacuados,
                   ROUND(efetivo_presente * 100.0 / NULLIF(efetivo_organico, 0), 1) AS pct_efetivo,
                   necessidade_prioritaria,
                   necessidade_logistica,
                   timestamp_geracao AS ts_pessoal
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.pessoal
            ) WHERE rn = 1
        ),
        material_subunidade AS (
            SELECT batalhao_origem, subunidade,
                   COUNT(DISTINCT id_viatura)                                                       AS viaturas_total,
                   SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END)                 AS viaturas_operacionais,
                   ROUND(SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) * 100.0
                         / NULLIF(COUNT(DISTINCT id_viatura), 0), 1)                               AS pct_viaturas,
                   ROUND(AVG(nivel_combustivel_pct), 1)                                            AS combustivel_medio_pct
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY id_viatura
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.material
            ) WHERE rn = 1
            GROUP BY batalhao_origem, subunidade
        ),
        ultima_seg AS (
            SELECT batalhao_origem, subunidade,
                   nivel_ameaca,
                   tipo_ocorrencia AS ultima_ocorrencia,
                   timestamp_geracao AS ts_seg
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.seg_area
            ) WHERE rn = 1
        ),
        intel_recente AS (
            SELECT batalhao_origem,
                   COUNT(*) AS ameacas_4h,
                   MAX(timestamp_geracao) AS ts_ultimo_intel
            FROM lakehouse.silver.relt_intel
            WHERE timestamp_geracao >= current_timestamp() - INTERVAL 4 HOURS
            GROUP BY batalhao_origem
        ),
        fogos_ativos AS (
            SELECT batalhao_origem, subunidade,
                   SUM(CASE WHEN status_execucao IN ('SOLICITADO','APROVADO')
                       THEN 1 ELSE 0 END) AS pafs_ativos
            FROM lakehouse.silver.paf
            GROUP BY batalhao_origem, subunidade
        )
        SELECT
            p.batalhao_origem,
            p.subunidade,
            -- Logistica: pessoal (S1)
            p.situacao_operacional,
            p.efetivo_organico, p.efetivo_presente, p.pct_efetivo,
            p.baixas_combate, p.baixas_nao_combate, p.evacuados,
            p.necessidade_prioritaria, p.necessidade_logistica, p.ts_pessoal,
            -- Logistica: material (S4)
            mat.viaturas_operacionais, mat.viaturas_total, mat.pct_viaturas,
            mat.combustivel_medio_pct,
            -- Manobra
            pos.latitude, pos.longitude, pos.velocidade, pos.direcao, pos.ts_gps,
            -- Protecao
            seg.nivel_ameaca, seg.ultima_ocorrencia, seg.ts_seg,
            -- Inteligencia
            COALESCE(i.ameacas_4h, 0) AS ameacas_4h, i.ts_ultimo_intel,
            -- Fogos
            COALESCE(f.pafs_ativos, 0) AS pafs_ativos,
            current_timestamp() AS atualizado_em
        FROM ultimo_pessoal p
        LEFT JOIN material_subunidade mat ON p.batalhao_origem = mat.batalhao_origem
                                         AND p.subunidade      = mat.subunidade
        LEFT JOIN ultima_posicao pos ON p.batalhao_origem = pos.batalhao_origem
                                    AND p.subunidade      = pos.subunidade
        LEFT JOIN ultima_seg seg     ON p.batalhao_origem = seg.batalhao_origem
                                    AND p.subunidade      = seg.subunidade
        LEFT JOIN intel_recente i    ON p.batalhao_origem = i.batalhao_origem
        LEFT JOIN fogos_ativos f     ON p.batalhao_origem = f.batalhao_origem
                                    AND p.subunidade      = f.subunidade
    """)
    _comentar_tabela(spark, "lakehouse.gold.coc",
        "Cenario Operacional Comum — visao integrada do estado atual da operacao por subunidade. "
        "Materializa a funcao integradora do C2 cruzando as 6 Funcoes de Combate: Logistica (S1 e S4), "
        "Manobra (GPS), Protecao, Inteligencia e Fogos. "
        "Produto da gestao do conhecimento previsto no EB70-MC-10.205."
    )
    _comentar(spark, "lakehouse.gold.coc", {
        "batalhao_origem":         "Sigla do batalhao (chave de join entre todas as Funcoes de Combate)",
        "subunidade":              "Subunidade organica (granularidade da visao COC)",
        "situacao_operacional":    "Prontidao operacional declarada pelo S1: OPERACIONAL, DEGRADADO, INOPERANTE, RESERVA",
        "efetivo_organico":        "Total de militares previsto no quadro organico da subunidade",
        "efetivo_presente":        "Total de militares prestos ao servico no momento do ultimo relatorio S1",
        "pct_efetivo":             "Percentual do efetivo organico presente — MEF de pessoal (Logistica S1)",
        "baixas_combate":          "Militares perdidos em acao de combate direta",
        "baixas_nao_combate":      "Militares perdidos por acidentes, doencas ou causas nao relacionadas ao combate",
        "evacuados":               "Militares afastados para atendimento medico externo",
        "necessidade_prioritaria": "Necessidade S1 mais urgente: PESSOAL_REFORCADO, EVACUACAO_MEDICA, NENHUMA",
        "necessidade_logistica":   "Necessidade S4 declarada: MUNICAO, COMBUSTIVEL, RACOES, MATERIAL SAUDE, etc.",
        "ts_pessoal":              "Timestamp do ultimo relatorio S1 recebido para esta subunidade",
        "viaturas_operacionais":   "Viaturas prontas para emprego imediato (status OPERACIONAL)",
        "viaturas_total":          "Total de viaturas organicas da subunidade",
        "pct_viaturas":            "Percentual de viaturas operacionais — MEF de material (Logistica S4)",
        "combustivel_medio_pct":   "Nivel medio de combustivel das viaturas da subunidade (%)",
        "latitude":                "Latitude da ultima posicao conhecida da subunidade (WGS-84, Manobra)",
        "longitude":               "Longitude da ultima posicao conhecida da subunidade (WGS-84, Manobra)",
        "velocidade":              "Velocidade da viatura no ultimo sinal GPS recebido (km/h)",
        "direcao":                 "Rumo magnetico no ultimo sinal GPS recebido (0 a 359 graus)",
        "ts_gps":                  "Timestamp do ultimo sinal GPS recebido para esta subunidade",
        "nivel_ameaca":            "Grau de ameaca da ultima ocorrencia de seguranca de area (Protecao)",
        "ultima_ocorrencia":       "Tipo da ultima ocorrencia de seguranca registrada para esta subunidade",
        "ts_seg":                  "Timestamp da ultima ocorrencia de seguranca registrada",
        "ameacas_4h":              "Quantidade de relatorios de inteligencia sobre o inimigo nas ultimas 4 horas (Inteligencia)",
        "ts_ultimo_intel":         "Timestamp do ultimo relatorio de inteligencia recebido para o batalhao",
        "pafs_ativos":             "Pedidos de Apoio de Fogo com status SOLICITADO ou APROVADO (Fogos)",
        "atualizado_em":           "Timestamp de geracao desta visao — referencia para staleness do dado",
    })
    df = spark.table("lakehouse.gold.coc")
    print(f"Gold COC: {df.count()} subunidades integradas")


# ---------------------------------------------------------------------------
# Visão 2 — PITCIC
# Análise do ambiente operacional por batalhão, organizada pelas 4 fases do
# Processo de Integração Terreno/Met./Inimigo/Civis (EB70-MC-10.336):
#   Fase 1 — Ambiente operacional: obstáculos + cobertura de sensor
#   Fase 2 — Efeitos do ambiente: transitabilidade + visibilidade sensor
#   Fase 3 — Avaliação da ameaça: relt_intel + seg_area
#   Fase 4 — Linhas de ação do inimigo: alvos PAF + efetivo estimado
# ---------------------------------------------------------------------------
def gold_pitcic(spark: SparkSession):
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.pitcic AS
        WITH batalhoes AS (
            SELECT DISTINCT batalhao_origem FROM lakehouse.silver.relt_intel
            UNION SELECT DISTINCT batalhao_origem FROM lakehouse.silver.obstaculo
            UNION SELECT DISTINCT batalhao_origem FROM lakehouse.silver.sensor
            UNION SELECT DISTINCT batalhao_origem FROM lakehouse.silver.seg_area
            UNION SELECT DISTINCT batalhao_origem FROM lakehouse.silver.paf
        ),
        terreno AS (
            SELECT batalhao_origem,
                   COUNT(*) AS total_obstaculos,
                   SUM(CASE WHEN transitabilidade = 'intransponivel' THEN 1 ELSE 0 END)
                       AS obstaculos_intransponiveis,
                   SUM(CASE WHEN coberto_fogo THEN 1 ELSE 0 END)
                       AS obstaculos_cobertos_fogo,
                   SUM(CASE WHEN confirmado_engenharia THEN 1 ELSE 0 END)
                       AS obstaculos_confirmados
            FROM lakehouse.silver.obstaculo
            GROUP BY batalhao_origem
        ),
        sensores AS (
            SELECT batalhao_origem,
                   COUNT(DISTINCT area_cobertura) AS areas_monitoradas,
                   SUM(CASE WHEN status_missao = 'ativo' THEN 1 ELSE 0 END) AS sensores_ativos,
                   ROUND(AVG(bateria_pct), 1) AS bateria_media_pct
            FROM lakehouse.silver.sensor
            GROUP BY batalhao_origem
        ),
        ameacas AS (
            SELECT batalhao_origem,
                   COUNT(*) AS total_ameacas,
                   SUM(CASE WHEN confiabilidade IN ('A','B') THEN 1 ELSE 0 END)
                       AS ameacas_alta_confiabilidade,
                   SUM(efetivo_estimado) AS efetivo_inimigo_estimado,
                   MAX(timestamp_geracao) AS ts_ultimo_intel
            FROM lakehouse.silver.relt_intel
            GROUP BY batalhao_origem
        ),
        ocorrencias AS (
            SELECT batalhao_origem,
                   MAX(nivel_ameaca) AS nivel_ameaca_max,
                   COUNT(*) AS total_ocorrencias_seg,
                   SUM(baixas_proprias) AS baixas_proprias_seg
            FROM lakehouse.silver.seg_area
            GROUP BY batalhao_origem
        ),
        fogos AS (
            SELECT batalhao_origem,
                   COUNT(DISTINCT tipo_alvo) AS tipos_alvo_distintos,
                   COUNT(*) AS total_missoes_fogo,
                   SUM(CASE WHEN status_execucao = 'EXECUTADO' THEN 1 ELSE 0 END)
                       AS fogos_executados
            FROM lakehouse.silver.paf
            GROUP BY batalhao_origem
        )
        SELECT
            b.batalhao_origem,
            -- Fase 1+2: Terreno e sensores
            COALESCE(t.total_obstaculos, 0)            AS total_obstaculos,
            COALESCE(t.obstaculos_intransponiveis, 0)  AS obstaculos_intransponiveis,
            COALESCE(t.obstaculos_cobertos_fogo, 0)    AS obstaculos_cobertos_fogo,
            COALESCE(t.obstaculos_confirmados, 0)      AS obstaculos_confirmados,
            COALESCE(s.areas_monitoradas, 0)           AS areas_monitoradas,
            COALESCE(s.sensores_ativos, 0)             AS sensores_ativos,
            s.bateria_media_pct,
            -- Fase 3: Avaliacao da ameaca
            COALESCE(a.total_ameacas, 0)               AS total_ameacas_intel,
            COALESCE(a.ameacas_alta_confiabilidade, 0) AS ameacas_alta_confiabilidade,
            COALESCE(a.efetivo_inimigo_estimado, 0)    AS efetivo_inimigo_estimado,
            a.ts_ultimo_intel,
            o.nivel_ameaca_max,
            COALESCE(o.total_ocorrencias_seg, 0)       AS total_ocorrencias_seg,
            COALESCE(o.baixas_proprias_seg, 0)         AS baixas_proprias_seg,
            -- Fase 4: Linhas de acao do inimigo
            COALESCE(f.tipos_alvo_distintos, 0)        AS tipos_alvo_distintos,
            COALESCE(f.total_missoes_fogo, 0)          AS total_missoes_fogo,
            COALESCE(f.fogos_executados, 0)            AS fogos_executados,
            current_timestamp() AS atualizado_em
        FROM batalhoes b
        LEFT JOIN terreno t   ON b.batalhao_origem = t.batalhao_origem
        LEFT JOIN sensores s  ON b.batalhao_origem = s.batalhao_origem
        LEFT JOIN ameacas a   ON b.batalhao_origem = a.batalhao_origem
        LEFT JOIN ocorrencias o ON b.batalhao_origem = o.batalhao_origem
        LEFT JOIN fogos f     ON b.batalhao_origem = f.batalhao_origem
    """)
    _comentar_tabela(spark, "lakehouse.gold.pitcic",
        "Processo de Integracao Terreno/Met./Inimigo/Civis — analise do ambiente operacional por batalhao. "
        "Organizado pelas 4 fases doutrinárias do EB70-MC-10.336: Fase 1+2 (terreno e sensores), "
        "Fase 3 (avaliacao da ameaca) e Fase 4 (linhas de acao do inimigo)."
    )
    _comentar(spark, "lakehouse.gold.pitcic", {
        "batalhao_origem":              "Sigla do batalhao (granularidade da visao PITCIC)",
        "total_obstaculos":             "Total de obstaculos identificados no setor — Fase 1 PITCIC (ambiente operacional: terreno)",
        "obstaculos_intransponiveis":   "Obstaculos que bloqueiam completamente a progressao propria",
        "obstaculos_cobertos_fogo":     "Obstaculos sob fogo inimigo — custo tatico elevado de franqueamento",
        "obstaculos_confirmados":       "Obstaculos confirmados por reconhecimento de Engenharia",
        "areas_monitoradas":            "Setores geograficos distintos cobertos por drones — Fase 2 PITCIC (efeitos do ambiente)",
        "sensores_ativos":              "Drones com status ativo no momento da geracao da visao",
        "bateria_media_pct":            "Autonomia media remanescente dos drones em percentual",
        "total_ameacas_intel":          "Total de avistamentos de elementos inimigos — Fase 3 PITCIC (avaliacao da ameaca)",
        "ameacas_alta_confiabilidade":  "Avistamentos com fonte confiavel (confiabilidade A ou B)",
        "efetivo_inimigo_estimado":     "Total de militares inimigos estimados em todos os avistamentos",
        "ts_ultimo_intel":              "Timestamp do avistamento inimigo mais recente registrado",
        "nivel_ameaca_max":             "Nivel maximo de ameaca registrado nas ocorrencias de seguranca de area",
        "total_ocorrencias_seg":        "Total de incidentes de seguranca de area registrados",
        "baixas_proprias_seg":          "Baixas proprias acumuladas em ocorrencias de seguranca de area",
        "tipos_alvo_distintos":         "Variedade de categorias de alvos identificados nos PAF — Fase 4 PITCIC (linhas de acao do inimigo)",
        "total_missoes_fogo":           "Total de Pedidos de Apoio de Fogo emitidos pelo batalhao",
        "fogos_executados":             "Missoes de fogo efetivamente executadas",
        "atualizado_em":                "Timestamp de geracao desta visao",
    })
    df = spark.table("lakehouse.gold.pitcic")
    print(f"Gold PITCIC: {df.count()} batalhões")


# ---------------------------------------------------------------------------
# Visão 3 — PPCOT
# Insumos para o Exame de Situação do Comandante, pelas 6 fases do Processo
# de Planejamento e Condução das Op. Terrestres (EB70-MC-10.211).
# Por batalhão: capacidade própria, situação, inimigo, terreno, apoio, decisão.
# ---------------------------------------------------------------------------
def gold_ppcot(spark: SparkSession):
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.ppcot")
    spark.sql("""
        CREATE TABLE lakehouse.gold.ppcot AS
        WITH batalhoes AS (
            SELECT DISTINCT batalhao_origem FROM lakehouse.silver.pessoal
            UNION SELECT DISTINCT batalhao_origem FROM lakehouse.silver.gps
        ),
        forca_propria AS (
            SELECT batalhao_origem,
                   SUM(efetivo_presente)   AS efetivo_total_presente,
                   SUM(efetivo_organico)   AS efetivo_total_organico,
                   ROUND(SUM(efetivo_presente) * 100.0 / NULLIF(SUM(efetivo_organico), 0), 1)
                       AS pct_efetivo_batalhao,
                   SUM(baixas_combate)     AS total_baixas_combate,
                   SUM(baixas_nao_combate) AS total_baixas_nao_combate,
                   SUM(evacuados)          AS total_evacuados
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.pessoal
            ) WHERE rn = 1
            GROUP BY batalhao_origem
        ),
        material_batalhao AS (
            SELECT batalhao_origem,
                   COUNT(DISTINCT id_viatura)                                                       AS viaturas_total,
                   SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END)                 AS viaturas_operacionais,
                   ROUND(SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) * 100.0
                         / NULLIF(COUNT(DISTINCT id_viatura), 0), 1)                               AS pct_viaturas,
                   ROUND(AVG(nivel_combustivel_pct), 1)                                            AS combustivel_medio_pct
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY id_viatura
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.material
            ) WHERE rn = 1
            GROUP BY batalhao_origem
        ),
        ultima_situacao AS (
            SELECT batalhao_origem,
                   MAX(CASE WHEN situacao_operacional = 'INOPERANTE' THEN 3
                            WHEN situacao_operacional = 'DEGRADADO'  THEN 2
                            ELSE 1 END)                  AS prioridade_situacao,
                   MAX(situacao_operacional)              AS pior_situacao,
                   COUNT(DISTINCT subunidade)             AS subunidades_reportando
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.pessoal
            ) WHERE rn = 1
            GROUP BY batalhao_origem
        ),
        situacao_inimigo AS (
            SELECT batalhao_origem,
                   COUNT(*) AS relatorios_intel,
                   SUM(efetivo_estimado) AS efetivo_inimigo_total,
                   SUM(CASE WHEN confiabilidade IN ('A','B') THEN 1 ELSE 0 END)
                       AS intel_confirmada
            FROM lakehouse.silver.relt_intel
            GROUP BY batalhao_origem
        ),
        terreno_restricoes AS (
            SELECT batalhao_origem,
                   COUNT(*) AS total_obstaculos,
                   SUM(CASE WHEN transitabilidade = 'intransponivel' THEN 1 ELSE 0 END)
                       AS vias_bloqueadas
            FROM lakehouse.silver.obstaculo
            GROUP BY batalhao_origem
        ),
        apoio_fogos AS (
            SELECT batalhao_origem,
                   SUM(CASE WHEN status_execucao = 'APROVADO' THEN 1 ELSE 0 END)
                       AS fogos_aprovados,
                   SUM(CASE WHEN status_execucao = 'SOLICITADO' THEN 1 ELSE 0 END)
                       AS fogos_solicitados,
                   SUM(CASE WHEN status_execucao = 'EXECUTADO' THEN 1 ELSE 0 END)
                       AS fogos_executados
            FROM lakehouse.silver.paf
            GROUP BY batalhao_origem
        ),
        necessidades AS (
            SELECT batalhao_origem,
                   MAX(necessidade_prioritaria) AS necessidade_critica
            FROM (
                SELECT batalhao_origem, necessidade_prioritaria,
                       ROW_NUMBER() OVER (
                           PARTITION BY batalhao_origem
                           ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.pessoal
            ) WHERE rn = 1
            GROUP BY batalhao_origem
        )
        SELECT
            b.batalhao_origem,
            -- Fase 1+2: Analise da missao — pessoal (S1)
            COALESCE(fp.efetivo_total_presente, 0)    AS efetivo_total_presente,
            COALESCE(fp.efetivo_total_organico, 0)    AS efetivo_total_organico,
            fp.pct_efetivo_batalhao,
            COALESCE(fp.total_baixas_combate, 0)      AS total_baixas_combate,
            COALESCE(fp.total_baixas_nao_combate, 0)  AS total_baixas_nao_combate,
            COALESCE(fp.total_evacuados, 0)           AS total_evacuados,
            -- Fase 1+2: Analise da missao — material (S4)
            COALESCE(mat.viaturas_operacionais, 0)    AS viaturas_operacionais,
            COALESCE(mat.viaturas_total, 0)           AS viaturas_total,
            mat.pct_viaturas,
            mat.combustivel_medio_pct,
            us.pior_situacao                          AS situacao_mais_critica,
            COALESCE(us.subunidades_reportando, 0)    AS subunidades_reportando,
            -- Fase 3: Levantamento — situacao do inimigo
            COALESCE(si.relatorios_intel, 0)          AS relatorios_intel,
            COALESCE(si.efetivo_inimigo_total, 0)     AS efetivo_inimigo_total,
            COALESCE(si.intel_confirmada, 0)          AS intel_confirmada,
            -- Terreno
            COALESCE(tr.total_obstaculos, 0)          AS total_obstaculos,
            COALESCE(tr.vias_bloqueadas, 0)           AS vias_bloqueadas,
            -- Fase 4+5: Apoio de fogo disponivel
            COALESCE(af.fogos_aprovados, 0)           AS fogos_aprovados,
            COALESCE(af.fogos_solicitados, 0)         AS fogos_solicitados,
            COALESCE(af.fogos_executados, 0)          AS fogos_executados,
            -- Fase 6: Necessidade prioritaria (insumo para decisao)
            n.necessidade_critica,
            current_timestamp() AS atualizado_em
        FROM batalhoes b
        LEFT JOIN forca_propria fp      ON b.batalhao_origem = fp.batalhao_origem
        LEFT JOIN material_batalhao mat ON b.batalhao_origem = mat.batalhao_origem
        LEFT JOIN ultima_situacao us    ON b.batalhao_origem = us.batalhao_origem
        LEFT JOIN situacao_inimigo si   ON b.batalhao_origem = si.batalhao_origem
        LEFT JOIN terreno_restricoes tr ON b.batalhao_origem = tr.batalhao_origem
        LEFT JOIN apoio_fogos af        ON b.batalhao_origem = af.batalhao_origem
        LEFT JOIN necessidades n        ON b.batalhao_origem = n.batalhao_origem
    """)
    _comentar_tabela(spark, "lakehouse.gold.ppcot",
        "Planejamento e Conducao das Operacoes Terrestres — insumos para o Exame de Situacao do Comandante por batalhao. "
        "Cobre as 6 fases do EB70-MC-10.211: analise da missao (S1 e S4), situacao propria e do inimigo, "
        "terreno, apoio de fogos disponivel e insumos para a decisao do comandante."
    )
    _comentar(spark, "lakehouse.gold.ppcot", {
        "batalhao_origem":          "Sigla do batalhao (granularidade da visao PPCOT)",
        "efetivo_total_presente":   "Total de militares prestos em todas as subunidades — Fases 1+2 PPCOT (analise da missao e situacao das proprias forcas)",
        "efetivo_total_organico":   "Total de militares previstos no quadro organico do batalhao",
        "pct_efetivo_batalhao":     "Percentual do efetivo organico presente no batalhao (Logistica S1)",
        "total_baixas_combate":     "Total de baixas de combate em todas as subunidades do batalhao",
        "total_baixas_nao_combate": "Total de baixas nao combate em todas as subunidades",
        "total_evacuados":          "Total de evacuados medicos em todas as subunidades",
        "viaturas_operacionais":    "Viaturas operacionais no batalhao (Logistica S4)",
        "viaturas_total":           "Total de viaturas organicas do batalhao",
        "pct_viaturas":             "Percentual de viaturas operacionais no batalhao",
        "combustivel_medio_pct":    "Nivel medio de combustivel das viaturas do batalhao (%)",
        "situacao_mais_critica":    "Pior situacao operacional entre as subunidades — insumo para priorizacao pelo comandante",
        "subunidades_reportando":   "Quantidade de subunidades com relatorio S1 recebido",
        "relatorios_intel":         "Total de relatorios de inteligencia sobre o inimigo — Fase 3 PPCOT (situacao do inimigo)",
        "efetivo_inimigo_total":    "Total de militares inimigos estimados em todos os avistamentos",
        "intel_confirmada":         "Avistamentos confirmados por fonte de alta confiabilidade (A ou B)",
        "total_obstaculos":         "Total de obstaculos no setor — insumo para Linhas de Acao (terreno)",
        "vias_bloqueadas":          "Obstaculos intransitraveis que comprometem rotas de progressao",
        "fogos_aprovados":          "PAF aprovados aguardando execucao — Fases 4+5 PPCOT (apoio de fogos disponivel)",
        "fogos_solicitados":        "PAF aguardando aprovacao do escalao de apoio de fogo",
        "fogos_executados":         "Missoes de fogo efetivamente concluidas",
        "necessidade_critica":      "Necessidade logistica mais urgente do batalhao — insumo para Fase 6 PPCOT (decisao do comandante)",
        "atualizado_em":            "Timestamp de geracao desta visao",
    })
    df = spark.table("lakehouse.gold.ppcot")
    print(f"Gold PPCOT: {df.count()} batalhões")


# ---------------------------------------------------------------------------
# Visão 4 — Avaliação
# Monitoramento da execução: indicadores quantitativos e qualitativos por
# subunidade, conforme Capítulo V do EB70-MC-10.211 (Avaliação da Condução).
# Medidas de eficácia (MEF) e de desempenho (MED) por subunidade.
# ---------------------------------------------------------------------------
def gold_avaliacao(spark: SparkSession):
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.avaliacao")
    spark.sql("""
        CREATE TABLE lakehouse.gold.avaliacao AS
        WITH efetivo AS (
            SELECT batalhao_origem, subunidade,
                   efetivo_organico, efetivo_presente,
                   ROUND(efetivo_presente * 100.0 / NULLIF(efetivo_organico, 0), 1) AS pct_efetivo,
                   baixas_combate, baixas_nao_combate, evacuados,
                   baixas_combate + baixas_nao_combate + evacuados AS total_baixas,
                   necessidade_prioritaria,
                   timestamp_geracao AS ts_pessoal
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.pessoal
            ) WHERE rn = 1
        ),
        material_sub AS (
            SELECT batalhao_origem, subunidade,
                   COUNT(DISTINCT id_viatura)                                                       AS viaturas_total,
                   SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END)                 AS viaturas_operacionais,
                   ROUND(SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) * 100.0
                         / NULLIF(COUNT(DISTINCT id_viatura), 0), 1)                               AS pct_viaturas,
                   ROUND(AVG(nivel_combustivel_pct), 1)                                            AS combustivel_medio_pct
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY id_viatura
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.material
            ) WHERE rn = 1
            GROUP BY batalhao_origem, subunidade
        ),
        pafs AS (
            SELECT batalhao_origem, subunidade,
                   SUM(CASE WHEN status_execucao = 'EXECUTADO' THEN 1 ELSE 0 END)
                       AS pafs_executados,
                   SUM(CASE WHEN status_execucao IN ('SOLICITADO','APROVADO') THEN 1 ELSE 0 END)
                       AS pafs_pendentes,
                   COUNT(*) AS pafs_total
            FROM lakehouse.silver.paf
            GROUP BY batalhao_origem, subunidade
        ),
        ameacas AS (
            SELECT batalhao_origem,
                   COUNT(*) AS ameacas_4h
            FROM lakehouse.silver.relt_intel
            WHERE timestamp_geracao >= current_timestamp() - INTERVAL 4 HOURS
            GROUP BY batalhao_origem
        ),
        seg AS (
            SELECT batalhao_origem, subunidade,
                   nivel_ameaca,
                   SUM(baixas_proprias) AS baixas_seg
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY batalhao_origem, subunidade
                    ORDER BY timestamp_geracao DESC) AS rn
                FROM lakehouse.silver.seg_area
            ) WHERE rn = 1
            GROUP BY batalhao_origem, subunidade, nivel_ameaca
        )
        SELECT
            e.batalhao_origem,
            e.subunidade,
            -- MEF: Medidas de Eficacia (capacidade operacional)
            e.pct_efetivo                   AS mef_pct_efetivo,
            mat.pct_viaturas                AS mef_pct_viaturas,
            mat.combustivel_medio_pct       AS mef_pct_combustivel,
            COALESCE(pf.pafs_executados, 0) * 100.0
                / NULLIF(COALESCE(pf.pafs_total, 0), 0)
                                            AS mef_pct_fogos_executados,
            -- MED: Medidas de Desempenho (perdas e pressao)
            e.total_baixas           AS med_total_baixas,
            e.baixas_combate         AS med_baixas_combate,
            e.baixas_nao_combate     AS med_baixas_nao_combate,
            e.evacuados              AS med_evacuados,
            seg.nivel_ameaca         AS med_nivel_ameaca,
            COALESCE(a.ameacas_4h, 0) AS med_ameacas_4h,
            -- Detalhe de logistica: pessoal (S1)
            e.efetivo_organico, e.efetivo_presente,
            e.necessidade_prioritaria,
            -- Detalhe de logistica: material (S4)
            mat.viaturas_operacionais, mat.viaturas_total,
            COALESCE(pf.pafs_pendentes, 0) AS pafs_pendentes,
            e.ts_pessoal,
            current_timestamp() AS atualizado_em
        FROM efetivo e
        LEFT JOIN material_sub mat ON e.batalhao_origem = mat.batalhao_origem
                                  AND e.subunidade      = mat.subunidade
        LEFT JOIN pafs pf  ON e.batalhao_origem = pf.batalhao_origem
                           AND e.subunidade      = pf.subunidade
        LEFT JOIN ameacas a ON e.batalhao_origem = a.batalhao_origem
        LEFT JOIN seg      ON e.batalhao_origem = seg.batalhao_origem
                           AND e.subunidade      = seg.subunidade
    """)
    _comentar_tabela(spark, "lakehouse.gold.avaliacao",
        "Avaliacao e Monitoramento da Conducao — indicadores quantitativos por subunidade conforme "
        "Capitulo V do EB70-MC-10.211. Medidas de Eficacia (MEF: efetivo, viaturas, combustivel, fogos) "
        "e Medidas de Desempenho (MED: baixas, nivel de ameaca, pressao de inteligencia) "
        "para o Grupo de Trabalho de Avaliacao Continua."
    )
    _comentar(spark, "lakehouse.gold.avaliacao", {
        "batalhao_origem":          "Sigla do batalhao",
        "subunidade":               "Subunidade avaliada (granularidade da visao de Avaliacao da Conducao)",
        "mef_pct_efetivo":          "MEF — percentual do efetivo organico presente (capacidade operacional de pessoal)",
        "mef_pct_viaturas":         "MEF — percentual de viaturas operacionais (capacidade operacional de material)",
        "mef_pct_combustivel":      "MEF — nivel medio de combustivel das viaturas (%)",
        "mef_pct_fogos_executados": "MEF — percentual dos PAF efetivamente executados sobre o total solicitado (eficacia de fogos)",
        "med_total_baixas":         "MED — total de baixas (combate + nao combate + evacuados)",
        "med_baixas_combate":       "MED — baixas em acao de combate direta",
        "med_baixas_nao_combate":   "MED — baixas por acidente, doenca ou outras causas nao relacionadas ao combate",
        "med_evacuados":            "MED — militares afastados para atendimento medico externo",
        "med_nivel_ameaca":         "MED — nivel de ameaca da ultima ocorrencia de seguranca de area",
        "med_ameacas_4h":           "MED — avistamentos inimigos confirmados nas ultimas 4 horas (pressao de inteligencia)",
        "efetivo_organico":         "Total de militares previstos no quadro organico",
        "efetivo_presente":         "Total de militares prestos ao servico",
        "necessidade_prioritaria":  "Necessidade S1: PESSOAL_REFORCADO, EVACUACAO_MEDICA, NENHUMA",
        "viaturas_operacionais":    "Viaturas prontas para emprego",
        "viaturas_total":           "Total de viaturas organicas da subunidade",
        "pafs_pendentes":           "PAF com status SOLICITADO ou APROVADO — missoes de fogo em aberto",
        "ts_pessoal":               "Timestamp do ultimo relatorio S1 recebido",
        "atualizado_em":            "Timestamp de geracao desta visao",
    })
    df = spark.table("lakehouse.gold.avaliacao")
    print(f"Gold Avaliação: {df.count()} subunidades monitoradas")


VISOES = {
    "coc":       gold_coc,
    "pitcic":    gold_pitcic,
    "ppcot":     gold_ppcot,
    "avaliacao": gold_avaliacao,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--visao", choices=list(VISOES.keys()), required=True)
    args = parser.parse_args()

    spark = get_spark()
    criar_schema_gold(spark)
    VISOES[args.visao](spark)
    spark.stop()


if __name__ == "__main__":
    main()
