"""
Emitter de linhagem OpenLineage para OpenMetadata.

O Spark OpenLineage listener emite datasets com namespace derivado
do data source (hive://hive-metastore:9083) que o OM 1.12.5 não
consegue resolver para o serviço trino_lakehouse. Este módulo emite
eventos com namespace=dlh e nomes catalog.schema.table que o OM
resolve corretamente, incluindo facets de SQL e column lineage
para exibição nas arestas do grafo de linhagem.

Disparado automaticamente via on_success_callback dos DAGs.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)

OM_URL = os.environ.get("OPENMETADATA_URL", "http://openmetadata:8585")
OM_OL_ENDPOINT = f"{OM_URL}/api/v1/openlineage/lineage"
OM_JWT = os.environ.get("OM_INGESTION_BOT_JWT", "")
OL_NAMESPACE = "dlh"
OL_PRODUCER = "https://github.com/infra-dissertacao/lineage_emitter"
OL_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
_FACET_PRODUCER = OL_PRODUCER
_COL_LINEAGE_SCHEMA = "https://openlineage.io/spec/facets/1-0-2/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"
_SQL_SCHEMA = "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet"

# ---------------------------------------------------------------------------
# Mapeamento centralizado: task_id → (inputs, outputs, sql, column_lineage)
# column_lineage: {coluna_output: [(tabela_input, coluna_input), ...]}
# ---------------------------------------------------------------------------

_GPS_SQL = """MERGE INTO lakehouse.silver.gps AS target
USING (
    SELECT
        get_json_object(payload, '$.id_veiculo') AS id_registro,
        batalhao_origem,
        CAST(NULL AS STRING) AS subunidade,
        CAST(get_json_object(payload, '$.latitude') AS DOUBLE) AS latitude,
        CAST(get_json_object(payload, '$.longitude') AS DOUBLE) AS longitude,
        CAST(get_json_object(payload, '$.altitude_m') AS DOUBLE) AS altitude,
        CAST(get_json_object(payload, '$.velocidade_kmh') AS DOUBLE) AS velocidade,
        CAST(get_json_object(payload, '$.direcao_graus') AS DOUBLE) AS direcao,
        timestamp_geracao, timestamp_chegada, id_lote,
        CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
        timestamp_geracao > timestamp_chegada AS fora_de_ordem,
        current_timestamp() AS processado_em
    FROM lakehouse.bronze.dados
    WHERE tipo_dado = 'gps'
      AND CAST(get_json_object(payload, '$.latitude') AS DOUBLE) BETWEEN -90 AND 90
      AND CAST(get_json_object(payload, '$.longitude') AS DOUBLE) BETWEEN -180 AND 180
) AS source
ON target.id_registro = source.id_registro
WHEN NOT MATCHED THEN INSERT *"""


_SENSOR_SQL = """MERGE INTO lakehouse.silver.sensor AS target
USING (
    SELECT
        get_json_object(payload, '$.id_sensor') AS id_registro,
        batalhao_origem,
        CAST(NULL AS STRING) AS drone_id, CAST(NULL AS STRING) AS area_cobertura,
        CAST(NULL AS DOUBLE) AS latitude_centro, CAST(NULL AS DOUBLE) AS longitude_centro,
        CAST(NULL AS DOUBLE) AS raio_km, CAST(NULL AS DOUBLE) AS altitude_voo,
        CAST(NULL AS INTEGER) AS bateria_pct, CAST(NULL AS STRING) AS status_missao,
        timestamp_geracao, timestamp_chegada, id_lote,
        CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
        timestamp_geracao > timestamp_chegada AS fora_de_ordem,
        current_timestamp() AS processado_em
    FROM lakehouse.bronze.dados
    WHERE tipo_dado = 'sensor'
) AS source
ON target.id_registro = source.id_registro
WHEN NOT MATCHED THEN INSERT *"""

_MATERIAL_SQL = """INSERT INTO lakehouse.silver.material
SELECT
    get_json_object(payload, '$.id_viatura')                              AS id_viatura,
    batalhao_origem,
    get_json_object(payload, '$.subunidade')                              AS subunidade,
    get_json_object(payload, '$.tipo_viatura')                            AS tipo_viatura,
    get_json_object(payload, '$.status_viatura')                          AS status_viatura,
    CAST(get_json_object(payload, '$.nivel_combustivel_pct') AS INTEGER)  AS nivel_combustivel_pct,
    CAST(get_json_object(payload, '$.km_rodados') AS INTEGER)             AS km_rodados,
    CAST(get_json_object(payload, '$.proxima_manutencao_km') AS INTEGER)  AS proxima_manutencao_km,
    timestamp_geracao, timestamp_chegada, id_lote,
    CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
    timestamp_geracao > timestamp_chegada AS fora_de_ordem,
    current_timestamp() AS processado_em
FROM lakehouse.bronze.dados src
WHERE tipo_dado = 'material'
  AND NOT EXISTS (
      SELECT 1 FROM lakehouse.silver.material tgt
      WHERE tgt.id_viatura        = get_json_object(src.payload, '$.id_viatura')
        AND tgt.timestamp_geracao = src.timestamp_geracao
  )"""

_RELT_INTEL_SQL = """MERGE INTO lakehouse.silver.relt_intel AS target
USING (
    SELECT
        get_json_object(payload, '$.id_relatorio')                      AS id_relatorio,
        batalhao_origem,
        get_json_object(payload, '$.subunidade')                        AS subunidade,
        get_json_object(payload, '$.tipo_ameaca')                       AS tipo_ameaca,
        CAST(get_json_object(payload, '$.coordenada_lat') AS DOUBLE)    AS coordenada_lat,
        CAST(get_json_object(payload, '$.coordenada_lon') AS DOUBLE)    AS coordenada_lon,
        CAST(get_json_object(payload, '$.efetivo_estimado') AS INTEGER) AS efetivo_estimado,
        get_json_object(payload, '$.confiabilidade')                    AS confiabilidade,
        get_json_object(payload, '$.fonte_info')                        AS fonte_info,
        get_json_object(payload, '$.descricao')                         AS descricao,
        timestamp_geracao, timestamp_chegada, id_lote,
        CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
        current_timestamp() AS processado_em
    FROM lakehouse.bronze.dados
    WHERE tipo_dado = 'relt_intel'
) AS source
ON target.id_relatorio = source.id_relatorio
WHEN NOT MATCHED THEN INSERT *"""

_PAF_SQL = """MERGE INTO lakehouse.silver.paf AS target
USING (
    SELECT
        get_json_object(payload, '$.id_paf')                              AS id_paf,
        batalhao_origem,
        get_json_object(payload, '$.subunidade')                          AS subunidade,
        get_json_object(payload, '$.tipo_missao')                         AS tipo_missao,
        CAST(get_json_object(payload, '$.coordenada_alvo_lat') AS DOUBLE) AS coordenada_alvo_lat,
        CAST(get_json_object(payload, '$.coordenada_alvo_lon') AS DOUBLE) AS coordenada_alvo_lon,
        get_json_object(payload, '$.tipo_alvo')                           AS tipo_alvo,
        get_json_object(payload, '$.tipo_municao')                        AS tipo_municao,
        get_json_object(payload, '$.prioridade')                          AS prioridade,
        get_json_object(payload, '$.status_execucao')                     AS status_execucao,
        timestamp_geracao, timestamp_chegada, id_lote,
        CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
        current_timestamp() AS processado_em
    FROM lakehouse.bronze.dados
    WHERE tipo_dado = 'paf'
) AS source
ON target.id_paf = source.id_paf
WHEN NOT MATCHED THEN INSERT *"""

_OBSTACULO_SQL = """MERGE INTO lakehouse.silver.obstaculo AS target
USING (
    SELECT
        get_json_object(payload, '$.id_obstaculo')                           AS id_obstaculo,
        batalhao_origem,
        get_json_object(payload, '$.subunidade')                             AS subunidade,
        get_json_object(payload, '$.tipo_obstaculo')                         AS tipo_obstaculo,
        CAST(get_json_object(payload, '$.coordenada_lat') AS DOUBLE)         AS coordenada_lat,
        CAST(get_json_object(payload, '$.coordenada_lon') AS DOUBLE)         AS coordenada_lon,
        get_json_object(payload, '$.transitabilidade')                       AS transitabilidade,
        CAST(get_json_object(payload, '$.coberto_fogo') AS BOOLEAN)          AS coberto_fogo,
        CAST(get_json_object(payload, '$.largura_m') AS DOUBLE)              AS largura_m,
        CAST(get_json_object(payload, '$.confirmado_engenharia') AS BOOLEAN) AS confirmado_engenharia,
        timestamp_geracao, timestamp_chegada, id_lote,
        CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
        current_timestamp() AS processado_em
    FROM lakehouse.bronze.dados
    WHERE tipo_dado = 'obstaculo'
) AS source
ON target.id_obstaculo = source.id_obstaculo
WHEN NOT MATCHED THEN INSERT *"""

_SEG_AREA_SQL = """MERGE INTO lakehouse.silver.seg_area AS target
USING (
    SELECT
        get_json_object(payload, '$.id_ocorrencia')                              AS id_ocorrencia,
        batalhao_origem,
        get_json_object(payload, '$.subunidade')                                 AS subunidade,
        get_json_object(payload, '$.tipo_ocorrencia')                            AS tipo_ocorrencia,
        CAST(get_json_object(payload, '$.coordenada_lat') AS DOUBLE)             AS coordenada_lat,
        CAST(get_json_object(payload, '$.coordenada_lon') AS DOUBLE)             AS coordenada_lon,
        CAST(get_json_object(payload, '$.efetivo_proprio_envolvido') AS INTEGER) AS efetivo_proprio_envolvido,
        CAST(get_json_object(payload, '$.baixas_proprias') AS INTEGER)           AS baixas_proprias,
        CAST(get_json_object(payload, '$.baixas_inimigas') AS INTEGER)           AS baixas_inimigas,
        get_json_object(payload, '$.nivel_ameaca')                               AS nivel_ameaca,
        get_json_object(payload, '$.status_resolucao')                           AS status_resolucao,
        timestamp_geracao, timestamp_chegada, id_lote,
        CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
        current_timestamp() AS processado_em
    FROM lakehouse.bronze.dados
    WHERE tipo_dado = 'seg_area'
) AS source
ON target.id_ocorrencia = source.id_ocorrencia
WHEN NOT MATCHED THEN INSERT *"""

_PESSOAL_SQL = """INSERT INTO lakehouse.silver.pessoal
SELECT
    get_json_object(payload, '$.id_relatorio')                        AS id_relatorio,
    batalhao_origem,
    get_json_object(payload, '$.subunidade')                          AS subunidade,
    get_json_object(payload, '$.situacao_operacional')                AS situacao_operacional,
    CAST(get_json_object(payload, '$.efetivo_organico') AS INTEGER)   AS efetivo_organico,
    CAST(get_json_object(payload, '$.efetivo_presente') AS INTEGER)   AS efetivo_presente,
    CAST(get_json_object(payload, '$.baixas_combate') AS INTEGER)     AS baixas_combate,
    CAST(get_json_object(payload, '$.baixas_nao_combate') AS INTEGER) AS baixas_nao_combate,
    CAST(get_json_object(payload, '$.evacuados') AS INTEGER)          AS evacuados,
    get_json_object(payload, '$.necessidade_prioritaria')             AS necessidade_prioritaria,
    get_json_object(payload, '$.necessidade_logistica')               AS necessidade_logistica,
    timestamp_geracao, timestamp_chegada, id_lote,
    CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
    current_timestamp() AS processado_em
FROM lakehouse.bronze.dados src
WHERE tipo_dado = 'pessoal'
  AND NOT EXISTS (
      SELECT 1 FROM lakehouse.silver.pessoal tgt
      WHERE tgt.id_relatorio = get_json_object(src.payload, '$.id_relatorio')
  )"""

_BRONZE = "lakehouse.bronze.dados"
_GPS = "lakehouse.silver.gps"
_SENSOR = "lakehouse.silver.sensor"
_RELT_INTEL = "lakehouse.silver.relt_intel"
_PAF = "lakehouse.silver.paf"
_OBSTACULO = "lakehouse.silver.obstaculo"
_SEG_AREA = "lakehouse.silver.seg_area"
_PESSOAL = "lakehouse.silver.pessoal"
_MATERIAL = "lakehouse.silver.material"
_GOLD_COC = "lakehouse.gold.coc"
_GOLD_PITCIC = "lakehouse.gold.pitcic"
_GOLD_PPCOT = "lakehouse.gold.ppcot"
_GOLD_AVALIACAO = "lakehouse.gold.avaliacao"

# ---------------------------------------------------------------------------
# SQL equivalente das visões Gold (reflete a lógica PySpark do silver_to_gold.py)
# ---------------------------------------------------------------------------

_GOLD_COC_SQL = """CREATE TABLE lakehouse.gold.coc AS
WITH ultimo_pessoal AS (
    SELECT batalhao_origem, subunidade,
           efetivo_organico, efetivo_presente,
           baixas_combate, baixas_nao_combate, evacuados,
           ROUND(efetivo_presente * 100.0 / NULLIF(efetivo_organico, 0), 1) AS pct_efetivo,
           necessidade_prioritaria,
           timestamp_geracao AS ts_pessoal
    FROM (SELECT *, ROW_NUMBER() OVER (
              PARTITION BY batalhao_origem, subunidade
              ORDER BY timestamp_geracao DESC) AS rn
          FROM lakehouse.silver.pessoal) WHERE rn = 1
),
material_subunidade AS (
    SELECT batalhao_origem, subunidade,
           COUNT(DISTINCT id_viatura) AS viaturas_total,
           SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) AS viaturas_operacionais,
           ROUND(SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) * 100.0
                 / NULLIF(COUNT(DISTINCT id_viatura), 0), 1) AS pct_viaturas,
           ROUND(AVG(nivel_combustivel_pct), 1) AS combustivel_medio_pct
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_viatura ORDER BY timestamp_geracao DESC) AS rn
          FROM lakehouse.silver.material) WHERE rn = 1
    GROUP BY batalhao_origem, subunidade
),
ultima_posicao AS (
    SELECT batalhao_origem, subunidade, latitude, longitude, velocidade, direcao,
           timestamp_geracao AS ts_gps
    FROM (SELECT *, ROW_NUMBER() OVER (
              PARTITION BY batalhao_origem, subunidade
              ORDER BY timestamp_geracao DESC) AS rn
          FROM lakehouse.silver.gps) WHERE rn = 1
),
ultima_seg AS (
    SELECT batalhao_origem, subunidade, nivel_ameaca,
           tipo_ocorrencia AS ultima_ocorrencia, timestamp_geracao AS ts_seg
    FROM (SELECT *, ROW_NUMBER() OVER (
              PARTITION BY batalhao_origem, subunidade
              ORDER BY timestamp_geracao DESC) AS rn
          FROM lakehouse.silver.seg_area) WHERE rn = 1
),
intel_recente AS (
    SELECT batalhao_origem, COUNT(*) AS ameacas_4h, MAX(timestamp_geracao) AS ts_ultimo_intel
    FROM lakehouse.silver.relt_intel
    WHERE timestamp_geracao >= current_timestamp() - INTERVAL 4 HOURS
    GROUP BY batalhao_origem
),
fogos_ativos AS (
    SELECT batalhao_origem, subunidade,
           SUM(CASE WHEN status_execucao IN ('SOLICITADO','APROVADO') THEN 1 ELSE 0 END) AS pafs_ativos
    FROM lakehouse.silver.paf GROUP BY batalhao_origem, subunidade
)
SELECT
    p.batalhao_origem, p.subunidade,
    p.situacao_operacional,
    p.efetivo_organico, p.efetivo_presente, p.pct_efetivo,
    p.baixas_combate, p.baixas_nao_combate, p.evacuados,
    p.necessidade_prioritaria, p.necessidade_logistica, p.ts_pessoal,
    mat.viaturas_operacionais, mat.viaturas_total, mat.pct_viaturas, mat.combustivel_medio_pct,
    pos.latitude, pos.longitude, pos.velocidade, pos.direcao, pos.ts_gps,
    seg.nivel_ameaca, seg.ultima_ocorrencia, seg.ts_seg,
    COALESCE(i.ameacas_4h, 0) AS ameacas_4h, i.ts_ultimo_intel,
    COALESCE(f.pafs_ativos, 0) AS pafs_ativos,
    current_timestamp() AS atualizado_em
FROM ultimo_pessoal p
LEFT JOIN material_subunidade mat ON p.batalhao_origem = mat.batalhao_origem AND p.subunidade = mat.subunidade
LEFT JOIN ultima_posicao pos ON p.batalhao_origem = pos.batalhao_origem AND p.subunidade = pos.subunidade
LEFT JOIN ultima_seg seg     ON p.batalhao_origem = seg.batalhao_origem AND p.subunidade = seg.subunidade
LEFT JOIN intel_recente i    ON p.batalhao_origem = i.batalhao_origem
LEFT JOIN fogos_ativos f     ON p.batalhao_origem = f.batalhao_origem AND p.subunidade = f.subunidade"""

_GOLD_PITCIC_SQL = """CREATE OR REPLACE TABLE lakehouse.gold.pitcic AS
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
           SUM(CASE WHEN transitabilidade = 'intransponivel' THEN 1 ELSE 0 END) AS obstaculos_intransponiveis,
           SUM(CASE WHEN coberto_fogo THEN 1 ELSE 0 END) AS obstaculos_cobertos_fogo,
           SUM(CASE WHEN confirmado_engenharia THEN 1 ELSE 0 END) AS obstaculos_confirmados
    FROM lakehouse.silver.obstaculo GROUP BY batalhao_origem
),
sensores AS (
    SELECT batalhao_origem,
           COUNT(DISTINCT area_cobertura) AS areas_monitoradas,
           SUM(CASE WHEN status_missao = 'ativo' THEN 1 ELSE 0 END) AS sensores_ativos,
           ROUND(AVG(bateria_pct), 1) AS bateria_media_pct
    FROM lakehouse.silver.sensor GROUP BY batalhao_origem
),
ameacas AS (
    SELECT batalhao_origem,
           COUNT(*) AS total_ameacas,
           SUM(CASE WHEN confiabilidade IN ('A','B') THEN 1 ELSE 0 END) AS ameacas_alta_confiabilidade,
           SUM(efetivo_estimado) AS efetivo_inimigo_estimado,
           MAX(timestamp_geracao) AS ts_ultimo_intel
    FROM lakehouse.silver.relt_intel GROUP BY batalhao_origem
),
ocorrencias AS (
    SELECT batalhao_origem,
           MAX(nivel_ameaca) AS nivel_ameaca_max,
           COUNT(*) AS total_ocorrencias_seg,
           SUM(baixas_proprias) AS baixas_proprias_seg
    FROM lakehouse.silver.seg_area GROUP BY batalhao_origem
),
fogos AS (
    SELECT batalhao_origem,
           COUNT(DISTINCT tipo_alvo) AS tipos_alvo_distintos,
           COUNT(*) AS total_missoes_fogo,
           SUM(CASE WHEN status_execucao = 'EXECUTADO' THEN 1 ELSE 0 END) AS fogos_executados
    FROM lakehouse.silver.paf GROUP BY batalhao_origem
)
SELECT
    b.batalhao_origem,
    COALESCE(t.total_obstaculos, 0)            AS total_obstaculos,
    COALESCE(t.obstaculos_intransponiveis, 0)  AS obstaculos_intransponiveis,
    COALESCE(t.obstaculos_cobertos_fogo, 0)    AS obstaculos_cobertos_fogo,
    COALESCE(t.obstaculos_confirmados, 0)      AS obstaculos_confirmados,
    COALESCE(s.areas_monitoradas, 0)           AS areas_monitoradas,
    COALESCE(s.sensores_ativos, 0)             AS sensores_ativos,
    s.bateria_media_pct,
    COALESCE(a.total_ameacas, 0)               AS total_ameacas_intel,
    COALESCE(a.ameacas_alta_confiabilidade, 0) AS ameacas_alta_confiabilidade,
    COALESCE(a.efetivo_inimigo_estimado, 0)    AS efetivo_inimigo_estimado,
    a.ts_ultimo_intel,
    o.nivel_ameaca_max,
    COALESCE(o.total_ocorrencias_seg, 0)       AS total_ocorrencias_seg,
    COALESCE(o.baixas_proprias_seg, 0)         AS baixas_proprias_seg,
    COALESCE(f.tipos_alvo_distintos, 0)        AS tipos_alvo_distintos,
    COALESCE(f.total_missoes_fogo, 0)          AS total_missoes_fogo,
    COALESCE(f.fogos_executados, 0)            AS fogos_executados,
    current_timestamp() AS atualizado_em
FROM batalhoes b
LEFT JOIN terreno t     ON b.batalhao_origem = t.batalhao_origem
LEFT JOIN sensores s    ON b.batalhao_origem = s.batalhao_origem
LEFT JOIN ameacas a     ON b.batalhao_origem = a.batalhao_origem
LEFT JOIN ocorrencias o ON b.batalhao_origem = o.batalhao_origem
LEFT JOIN fogos f       ON b.batalhao_origem = f.batalhao_origem"""

_GOLD_PPCOT_SQL = """CREATE TABLE lakehouse.gold.ppcot AS
WITH batalhoes AS (
    SELECT DISTINCT batalhao_origem FROM lakehouse.silver.pessoal
    UNION SELECT DISTINCT batalhao_origem FROM lakehouse.silver.gps
),
forca_propria AS (
    SELECT batalhao_origem,
           SUM(efetivo_presente)   AS efetivo_total_presente,
           SUM(efetivo_organico)   AS efetivo_total_organico,
           ROUND(SUM(efetivo_presente) * 100.0 / NULLIF(SUM(efetivo_organico), 0), 1) AS pct_efetivo_batalhao,
           SUM(baixas_combate)     AS total_baixas_combate,
           SUM(baixas_nao_combate) AS total_baixas_nao_combate,
           SUM(evacuados)          AS total_evacuados
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY batalhao_origem, subunidade ORDER BY timestamp_geracao DESC) AS rn
        FROM lakehouse.silver.pessoal
    ) WHERE rn = 1
    GROUP BY batalhao_origem
),
material_batalhao AS (
    SELECT batalhao_origem,
           COUNT(DISTINCT id_viatura) AS viaturas_total,
           SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) AS viaturas_operacionais,
           ROUND(SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) * 100.0
                 / NULLIF(COUNT(DISTINCT id_viatura), 0), 1) AS pct_viaturas,
           ROUND(AVG(nivel_combustivel_pct), 1) AS combustivel_medio_pct
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_viatura ORDER BY timestamp_geracao DESC) AS rn
          FROM lakehouse.silver.material) WHERE rn = 1
    GROUP BY batalhao_origem
),
ultima_situacao AS (
    SELECT batalhao_origem,
           MAX(situacao) AS pior_situacao,
           COUNT(DISTINCT subunidade) AS subunidades_reportando
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY batalhao_origem, subunidade ORDER BY timestamp_geracao DESC) AS rn
        FROM lakehouse.silver.pessoal
    ) WHERE rn = 1
    GROUP BY batalhao_origem
),
situacao_inimigo AS (
    SELECT batalhao_origem,
           COUNT(*) AS relatorios_intel,
           SUM(efetivo_estimado) AS efetivo_inimigo_total,
           SUM(CASE WHEN confiabilidade IN ('A','B') THEN 1 ELSE 0 END) AS intel_confirmada
    FROM lakehouse.silver.relt_intel GROUP BY batalhao_origem
),
terreno_restricoes AS (
    SELECT batalhao_origem,
           COUNT(*) AS total_obstaculos,
           SUM(CASE WHEN transitabilidade = 'intransponivel' THEN 1 ELSE 0 END) AS vias_bloqueadas
    FROM lakehouse.silver.obstaculo GROUP BY batalhao_origem
),
apoio_fogos AS (
    SELECT batalhao_origem,
           SUM(CASE WHEN status_execucao = 'APROVADO'   THEN 1 ELSE 0 END) AS fogos_aprovados,
           SUM(CASE WHEN status_execucao = 'SOLICITADO' THEN 1 ELSE 0 END) AS fogos_solicitados,
           SUM(CASE WHEN status_execucao = 'EXECUTADO'  THEN 1 ELSE 0 END) AS fogos_executados
    FROM lakehouse.silver.paf GROUP BY batalhao_origem
),
necessidades AS (
    SELECT batalhao_origem, MAX(necessidade_prioritaria) AS necessidade_critica
    FROM (
        SELECT batalhao_origem, necessidade_prioritaria,
               ROW_NUMBER() OVER (PARTITION BY batalhao_origem ORDER BY timestamp_geracao DESC) AS rn
        FROM lakehouse.silver.pessoal
    ) WHERE rn = 1
    GROUP BY batalhao_origem
)
SELECT
    b.batalhao_origem,
    COALESCE(fp.efetivo_total_presente, 0)   AS efetivo_total_presente,
    COALESCE(fp.efetivo_total_organico, 0)   AS efetivo_total_organico,
    fp.pct_efetivo_batalhao,
    COALESCE(fp.total_baixas_combate, 0)     AS total_baixas_combate,
    COALESCE(fp.total_baixas_nao_combate, 0) AS total_baixas_nao_combate,
    COALESCE(fp.total_evacuados, 0)          AS total_evacuados,
    COALESCE(mat.viaturas_operacionais, 0)   AS viaturas_operacionais,
    COALESCE(mat.viaturas_total, 0)          AS viaturas_total,
    mat.pct_viaturas, mat.combustivel_medio_pct,
    us.pior_situacao                         AS situacao_mais_critica,
    COALESCE(us.subunidades_reportando, 0)   AS subunidades_reportando,
    COALESCE(si.relatorios_intel, 0)         AS relatorios_intel,
    COALESCE(si.efetivo_inimigo_total, 0)    AS efetivo_inimigo_total,
    COALESCE(si.intel_confirmada, 0)         AS intel_confirmada,
    COALESCE(tr.total_obstaculos, 0)         AS total_obstaculos,
    COALESCE(tr.vias_bloqueadas, 0)          AS vias_bloqueadas,
    COALESCE(af.fogos_aprovados, 0)          AS fogos_aprovados,
    COALESCE(af.fogos_solicitados, 0)        AS fogos_solicitados,
    COALESCE(af.fogos_executados, 0)         AS fogos_executados,
    n.necessidade_critica,
    current_timestamp() AS atualizado_em
FROM batalhoes b
LEFT JOIN forca_propria fp      ON b.batalhao_origem = fp.batalhao_origem
LEFT JOIN material_batalhao mat ON b.batalhao_origem = mat.batalhao_origem
LEFT JOIN ultima_situacao us    ON b.batalhao_origem = us.batalhao_origem
LEFT JOIN situacao_inimigo si   ON b.batalhao_origem = si.batalhao_origem
LEFT JOIN terreno_restricoes tr ON b.batalhao_origem = tr.batalhao_origem
LEFT JOIN apoio_fogos af        ON b.batalhao_origem = af.batalhao_origem
LEFT JOIN necessidades n        ON b.batalhao_origem = n.batalhao_origem"""

_GOLD_AVALIACAO_SQL = """CREATE TABLE lakehouse.gold.avaliacao AS
WITH efetivo AS (
    SELECT batalhao_origem, subunidade,
           efetivo_organico, efetivo_presente,
           ROUND(efetivo_presente * 100.0 / NULLIF(efetivo_organico, 0), 1) AS pct_efetivo,
           baixas_combate, baixas_nao_combate, evacuados,
           baixas_combate + baixas_nao_combate + evacuados AS total_baixas,
           necessidade_prioritaria,
           timestamp_geracao AS ts_pessoal
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY batalhao_origem, subunidade ORDER BY timestamp_geracao DESC) AS rn
        FROM lakehouse.silver.pessoal
    ) WHERE rn = 1
),
material_sub AS (
    SELECT batalhao_origem, subunidade,
           COUNT(DISTINCT id_viatura) AS viaturas_total,
           SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) AS viaturas_operacionais,
           ROUND(SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) * 100.0
                 / NULLIF(COUNT(DISTINCT id_viatura), 0), 1) AS pct_viaturas,
           ROUND(AVG(nivel_combustivel_pct), 1) AS combustivel_medio_pct
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id_viatura ORDER BY timestamp_geracao DESC) AS rn
          FROM lakehouse.silver.material) WHERE rn = 1
    GROUP BY batalhao_origem, subunidade
),
pafs AS (
    SELECT batalhao_origem, subunidade,
           SUM(CASE WHEN status_execucao = 'EXECUTADO' THEN 1 ELSE 0 END) AS pafs_executados,
           SUM(CASE WHEN status_execucao IN ('SOLICITADO','APROVADO') THEN 1 ELSE 0 END) AS pafs_pendentes,
           COUNT(*) AS pafs_total
    FROM lakehouse.silver.paf GROUP BY batalhao_origem, subunidade
),
ameacas AS (
    SELECT batalhao_origem, COUNT(*) AS ameacas_4h
    FROM lakehouse.silver.relt_intel
    WHERE timestamp_geracao >= current_timestamp() - INTERVAL 4 HOURS
    GROUP BY batalhao_origem
),
seg AS (
    SELECT batalhao_origem, subunidade, nivel_ameaca, SUM(baixas_proprias) AS baixas_seg
    FROM (SELECT *, ROW_NUMBER() OVER (
              PARTITION BY batalhao_origem, subunidade ORDER BY timestamp_geracao DESC) AS rn
          FROM lakehouse.silver.seg_area) WHERE rn = 1
    GROUP BY batalhao_origem, subunidade, nivel_ameaca
)
SELECT
    e.batalhao_origem, e.subunidade,
    e.pct_efetivo                AS mef_pct_efetivo,
    mat.pct_viaturas             AS mef_pct_viaturas,
    mat.combustivel_medio_pct    AS mef_pct_combustivel,
    COALESCE(pf.pafs_executados, 0) * 100.0 / NULLIF(COALESCE(pf.pafs_total, 0), 0) AS mef_pct_fogos_executados,
    e.total_baixas               AS med_total_baixas,
    e.baixas_combate             AS med_baixas_combate,
    e.baixas_nao_combate         AS med_baixas_nao_combate,
    e.evacuados                  AS med_evacuados,
    seg.nivel_ameaca             AS med_nivel_ameaca,
    COALESCE(a.ameacas_4h, 0)    AS med_ameacas_4h,
    e.efetivo_organico, e.efetivo_presente,
    mat.viaturas_operacionais, mat.viaturas_total,
    e.necessidade_prioritaria,
    COALESCE(pf.pafs_pendentes, 0) AS pafs_pendentes,
    e.ts_pessoal,
    current_timestamp() AS atualizado_em
FROM efetivo e
LEFT JOIN material_sub mat ON e.batalhao_origem = mat.batalhao_origem AND e.subunidade = mat.subunidade
LEFT JOIN pafs pf  ON e.batalhao_origem = pf.batalhao_origem AND e.subunidade = pf.subunidade
LEFT JOIN ameacas a ON e.batalhao_origem = a.batalhao_origem
LEFT JOIN seg      ON e.batalhao_origem = seg.batalhao_origem AND e.subunidade = seg.subunidade"""

LINEAGE_MAP: dict[str, dict] = {
    "ingerir_json_para_iceberg_bronze": {
        "inputs": [],
        "outputs": [_BRONZE],
    },
    "silver_gps": {
        "inputs": [_BRONZE],
        "outputs": ["lakehouse.silver.gps"],
        "sql": _GPS_SQL,
        "column_lineage": {
            "id_registro": [(_BRONZE, "payload")],
            "batalhao_origem": [(_BRONZE, "batalhao_origem")],
            "latitude": [(_BRONZE, "payload")],
            "longitude": [(_BRONZE, "payload")],
            "altitude": [(_BRONZE, "payload")],
            "velocidade": [(_BRONZE, "payload")],
            "direcao": [(_BRONZE, "payload")],
            "timestamp_geracao": [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada": [(_BRONZE, "timestamp_chegada")],
            "id_lote": [(_BRONZE, "id_lote")],
            "latencia_ingestao_s": [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
            "fora_de_ordem": [(_BRONZE, "timestamp_geracao"), (_BRONZE, "timestamp_chegada")],
        },
    },
    "silver_sensor": {
        "inputs": [_BRONZE],
        "outputs": ["lakehouse.silver.sensor"],
        "sql": _SENSOR_SQL,
        "column_lineage": {
            "id_registro": [(_BRONZE, "payload")],
            "batalhao_origem": [(_BRONZE, "batalhao_origem")],
            "timestamp_geracao": [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada": [(_BRONZE, "timestamp_chegada")],
            "id_lote": [(_BRONZE, "id_lote")],
            "latencia_ingestao_s": [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
            "fora_de_ordem": [(_BRONZE, "timestamp_geracao"), (_BRONZE, "timestamp_chegada")],
        },
    },
    # --- Novas Funções de Combate ---
    "silver_relt_intel": {
        "inputs": [_BRONZE],
        "outputs": [_RELT_INTEL],
        "sql": _RELT_INTEL_SQL,
        "column_lineage": {
            "id_relatorio":     [(_BRONZE, "payload")],
            "batalhao_origem":  [(_BRONZE, "batalhao_origem")],
            "subunidade":       [(_BRONZE, "payload")],
            "tipo_ameaca":      [(_BRONZE, "payload")],
            "coordenada_lat":   [(_BRONZE, "payload")],
            "coordenada_lon":   [(_BRONZE, "payload")],
            "efetivo_estimado": [(_BRONZE, "payload")],
            "confiabilidade":   [(_BRONZE, "payload")],
            "fonte_info":       [(_BRONZE, "payload")],
            "timestamp_geracao": [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada": [(_BRONZE, "timestamp_chegada")],
            "latencia_ingestao_s": [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
        },
    },
    "silver_paf": {
        "inputs": [_BRONZE],
        "outputs": [_PAF],
        "sql": _PAF_SQL,
        "column_lineage": {
            "id_paf":              [(_BRONZE, "payload")],
            "batalhao_origem":     [(_BRONZE, "batalhao_origem")],
            "subunidade":          [(_BRONZE, "payload")],
            "tipo_missao":         [(_BRONZE, "payload")],
            "coordenada_alvo_lat": [(_BRONZE, "payload")],
            "coordenada_alvo_lon": [(_BRONZE, "payload")],
            "tipo_alvo":           [(_BRONZE, "payload")],
            "prioridade":          [(_BRONZE, "payload")],
            "status_execucao":     [(_BRONZE, "payload")],
            "timestamp_geracao":   [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada":   [(_BRONZE, "timestamp_chegada")],
            "latencia_ingestao_s": [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
        },
    },
    "silver_obstaculo": {
        "inputs": [_BRONZE],
        "outputs": [_OBSTACULO],
        "sql": _OBSTACULO_SQL,
        "column_lineage": {
            "id_obstaculo":          [(_BRONZE, "payload")],
            "batalhao_origem":       [(_BRONZE, "batalhao_origem")],
            "subunidade":            [(_BRONZE, "payload")],
            "tipo_obstaculo":        [(_BRONZE, "payload")],
            "coordenada_lat":        [(_BRONZE, "payload")],
            "coordenada_lon":        [(_BRONZE, "payload")],
            "transitabilidade":      [(_BRONZE, "payload")],
            "coberto_fogo":          [(_BRONZE, "payload")],
            "largura_m":             [(_BRONZE, "payload")],
            "confirmado_engenharia": [(_BRONZE, "payload")],
            "timestamp_geracao":     [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada":     [(_BRONZE, "timestamp_chegada")],
            "latencia_ingestao_s":   [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
        },
    },
    "silver_seg_area": {
        "inputs": [_BRONZE],
        "outputs": [_SEG_AREA],
        "sql": _SEG_AREA_SQL,
        "column_lineage": {
            "id_ocorrencia":              [(_BRONZE, "payload")],
            "batalhao_origem":            [(_BRONZE, "batalhao_origem")],
            "subunidade":                 [(_BRONZE, "payload")],
            "tipo_ocorrencia":            [(_BRONZE, "payload")],
            "efetivo_proprio_envolvido":  [(_BRONZE, "payload")],
            "baixas_proprias":            [(_BRONZE, "payload")],
            "nivel_ameaca":               [(_BRONZE, "payload")],
            "status_resolucao":           [(_BRONZE, "payload")],
            "timestamp_geracao":          [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada":          [(_BRONZE, "timestamp_chegada")],
            "latencia_ingestao_s":        [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
        },
    },
    "silver_pessoal": {
        "inputs": [_BRONZE],
        "outputs": [_PESSOAL],
        "sql": _PESSOAL_SQL,
        "column_lineage": {
            "id_relatorio":             [(_BRONZE, "payload")],
            "batalhao_origem":          [(_BRONZE, "batalhao_origem")],
            "subunidade":               [(_BRONZE, "payload")],
            "efetivo_organico":         [(_BRONZE, "payload")],
            "efetivo_presente":         [(_BRONZE, "payload")],
            "baixas_combate":           [(_BRONZE, "payload")],
            "baixas_nao_combate":       [(_BRONZE, "payload")],
            "evacuados":                [(_BRONZE, "payload")],
            "necessidade_prioritaria":  [(_BRONZE, "payload")],
            "timestamp_geracao":        [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada":        [(_BRONZE, "timestamp_chegada")],
            "latencia_ingestao_s":      [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
        },
    },
    "silver_material": {
        "inputs": [_BRONZE],
        "outputs": [_MATERIAL],
        "sql": _MATERIAL_SQL,
        "column_lineage": {
            "id_viatura":              [(_BRONZE, "payload")],
            "batalhao_origem":         [(_BRONZE, "batalhao_origem")],
            "subunidade":              [(_BRONZE, "payload")],
            "tipo_viatura":            [(_BRONZE, "payload")],
            "status_viatura":          [(_BRONZE, "payload")],
            "nivel_combustivel_pct":   [(_BRONZE, "payload")],
            "km_rodados":              [(_BRONZE, "payload")],
            "proxima_manutencao_km":   [(_BRONZE, "payload")],
            "timestamp_geracao":       [(_BRONZE, "timestamp_geracao")],
            "timestamp_chegada":       [(_BRONZE, "timestamp_chegada")],
            "latencia_ingestao_s":     [(_BRONZE, "timestamp_chegada"), (_BRONZE, "timestamp_geracao")],
            "fora_de_ordem":           [(_BRONZE, "timestamp_geracao"), (_BRONZE, "timestamp_chegada")],
        },
    },
    # --- Gold (visões doutrinariamente orientadas) ---
    "gold_coc": {
        "inputs": [_GPS, _PESSOAL, _MATERIAL, _SEG_AREA, _RELT_INTEL, _PAF],
        "outputs": [_GOLD_COC],
        "sql": _GOLD_COC_SQL,
        "column_lineage": {
            # Logistica S1
            "batalhao_origem":         [(_PESSOAL, "batalhao_origem")],
            "subunidade":              [(_PESSOAL, "subunidade")],
            "efetivo_organico":        [(_PESSOAL, "efetivo_organico")],
            "efetivo_presente":        [(_PESSOAL, "efetivo_presente")],
            "pct_efetivo":             [(_PESSOAL, "efetivo_presente"), (_PESSOAL, "efetivo_organico")],
            "baixas_combate":          [(_PESSOAL, "baixas_combate")],
            "baixas_nao_combate":      [(_PESSOAL, "baixas_nao_combate")],
            "evacuados":               [(_PESSOAL, "evacuados")],
            "necessidade_prioritaria": [(_PESSOAL, "necessidade_prioritaria")],
            "ts_pessoal":              [(_PESSOAL, "timestamp_geracao")],
            # Logistica S4
            "viaturas_operacionais":   [(_MATERIAL, "status_viatura")],
            "viaturas_total":          [(_MATERIAL, "id_viatura")],
            "pct_viaturas":            [(_MATERIAL, "status_viatura"), (_MATERIAL, "id_viatura")],
            "combustivel_medio_pct":   [(_MATERIAL, "nivel_combustivel_pct")],
            # Logistica (S1) — campos migrados do sitrep extinto
            "situacao_operacional":    [(_PESSOAL, "situacao_operacional")],
            "necessidade_logistica":   [(_PESSOAL, "necessidade_logistica")],
            # Manobra / GPS
            "latitude":                [(_GPS, "latitude")],
            "longitude":               [(_GPS, "longitude")],
            "velocidade":              [(_GPS, "velocidade")],
            "direcao":                 [(_GPS, "direcao")],
            "ts_gps":                  [(_GPS, "timestamp_geracao")],
            # Proteção
            "nivel_ameaca":            [(_SEG_AREA, "nivel_ameaca")],
            "ultima_ocorrencia":       [(_SEG_AREA, "tipo_ocorrencia")],
            "ts_seg":                  [(_SEG_AREA, "timestamp_geracao")],
            # Inteligência
            "ameacas_4h":              [(_RELT_INTEL, "id_relatorio")],
            "ts_ultimo_intel":         [(_RELT_INTEL, "timestamp_geracao")],
            # Fogos
            "pafs_ativos":             [(_PAF, "status_execucao")],
        },
    },
    "gold_pitcic": {
        "inputs": [_OBSTACULO, _SENSOR, _RELT_INTEL, _SEG_AREA, _PAF],
        "outputs": [_GOLD_PITCIC],
        "sql": _GOLD_PITCIC_SQL,
        "column_lineage": {
            "batalhao_origem":               [(_OBSTACULO, "batalhao_origem")],
            "total_obstaculos":              [(_OBSTACULO, "id_obstaculo")],
            "obstaculos_intransponiveis":    [(_OBSTACULO, "transitabilidade")],
            "obstaculos_cobertos_fogo":      [(_OBSTACULO, "coberto_fogo")],
            "obstaculos_confirmados":        [(_OBSTACULO, "confirmado_engenharia")],
            "areas_monitoradas":             [(_SENSOR, "area_cobertura")],
            "sensores_ativos":               [(_SENSOR, "status_missao")],
            "bateria_media_pct":             [(_SENSOR, "bateria_pct")],
            "total_ameacas_intel":           [(_RELT_INTEL, "id_relatorio")],
            "ameacas_alta_confiabilidade":   [(_RELT_INTEL, "confiabilidade")],
            "efetivo_inimigo_estimado":      [(_RELT_INTEL, "efetivo_estimado")],
            "ts_ultimo_intel":               [(_RELT_INTEL, "timestamp_geracao")],
            "nivel_ameaca_max":              [(_SEG_AREA, "nivel_ameaca")],
            "total_ocorrencias_seg":         [(_SEG_AREA, "id_ocorrencia")],
            "baixas_proprias_seg":           [(_SEG_AREA, "baixas_proprias")],
            "tipos_alvo_distintos":          [(_PAF, "tipo_alvo")],
            "total_missoes_fogo":            [(_PAF, "id_paf")],
            "fogos_executados":              [(_PAF, "status_execucao")],
        },
    },
    "gold_ppcot": {
        "inputs": [_PESSOAL, _MATERIAL, _RELT_INTEL, _OBSTACULO, _PAF],
        "outputs": [_GOLD_PPCOT],
        "sql": _GOLD_PPCOT_SQL,
        "column_lineage": {
            # Logistica S1
            "batalhao_origem":           [(_PESSOAL, "batalhao_origem")],
            "efetivo_total_presente":    [(_PESSOAL, "efetivo_presente")],
            "efetivo_total_organico":    [(_PESSOAL, "efetivo_organico")],
            "pct_efetivo_batalhao":      [(_PESSOAL, "efetivo_presente"), (_PESSOAL, "efetivo_organico")],
            "total_baixas_combate":      [(_PESSOAL, "baixas_combate")],
            "total_baixas_nao_combate":  [(_PESSOAL, "baixas_nao_combate")],
            "total_evacuados":           [(_PESSOAL, "evacuados")],
            "necessidade_critica":       [(_PESSOAL, "necessidade_prioritaria")],
            # Logistica S4
            "viaturas_operacionais":     [(_MATERIAL, "status_viatura")],
            "viaturas_total":            [(_MATERIAL, "id_viatura")],
            "pct_viaturas":              [(_MATERIAL, "status_viatura"), (_MATERIAL, "id_viatura")],
            "combustivel_medio_pct":     [(_MATERIAL, "nivel_combustivel_pct")],
            # Situacao operacional (migrado do sitrep extinto)
            "situacao_mais_critica":     [(_PESSOAL, "situacao_operacional")],
            "subunidades_reportando":    [(_PESSOAL, "subunidade")],
            # Inimigo
            "relatorios_intel":          [(_RELT_INTEL, "id_relatorio")],
            "efetivo_inimigo_total":     [(_RELT_INTEL, "efetivo_estimado")],
            "intel_confirmada":          [(_RELT_INTEL, "confiabilidade")],
            # Terreno
            "total_obstaculos":          [(_OBSTACULO, "id_obstaculo")],
            "vias_bloqueadas":           [(_OBSTACULO, "transitabilidade")],
            # Fogos
            "fogos_aprovados":           [(_PAF, "status_execucao")],
            "fogos_solicitados":         [(_PAF, "status_execucao")],
            "fogos_executados":          [(_PAF, "status_execucao")],
        },
    },
    "gold_avaliacao": {
        "inputs": [_PESSOAL, _MATERIAL, _PAF, _RELT_INTEL, _SEG_AREA],
        "outputs": [_GOLD_AVALIACAO],
        "sql": _GOLD_AVALIACAO_SQL,
        "column_lineage": {
            # MEF — pessoal (S1)
            "batalhao_origem":          [(_PESSOAL, "batalhao_origem")],
            "subunidade":               [(_PESSOAL, "subunidade")],
            "mef_pct_efetivo":          [(_PESSOAL, "efetivo_presente"), (_PESSOAL, "efetivo_organico")],
            # MEF — material (S4)
            "mef_pct_viaturas":         [(_MATERIAL, "status_viatura"), (_MATERIAL, "id_viatura")],
            "mef_pct_combustivel":      [(_MATERIAL, "nivel_combustivel_pct")],
            # MEF — fogos
            "mef_pct_fogos_executados": [(_PAF, "status_execucao")],
            # MED — baixas
            "med_total_baixas":         [(_PESSOAL, "baixas_combate"), (_PESSOAL, "baixas_nao_combate"), (_PESSOAL, "evacuados")],
            "med_baixas_combate":       [(_PESSOAL, "baixas_combate")],
            "med_baixas_nao_combate":   [(_PESSOAL, "baixas_nao_combate")],
            "med_evacuados":            [(_PESSOAL, "evacuados")],
            "med_nivel_ameaca":         [(_SEG_AREA, "nivel_ameaca")],
            "med_ameacas_4h":           [(_RELT_INTEL, "id_relatorio")],
            # Detalhe pessoal
            "efetivo_organico":         [(_PESSOAL, "efetivo_organico")],
            "efetivo_presente":         [(_PESSOAL, "efetivo_presente")],
            "necessidade_prioritaria":  [(_PESSOAL, "necessidade_prioritaria")],
            "ts_pessoal":               [(_PESSOAL, "timestamp_geracao")],
            # Detalhe material
            "viaturas_operacionais":    [(_MATERIAL, "status_viatura")],
            "viaturas_total":           [(_MATERIAL, "id_viatura")],
            # Fogos pendentes
            "pafs_pendentes":           [(_PAF, "status_execucao")],
        },
    },
}


def _build_column_lineage_facet(col_map: dict[str, list[tuple[str, str]]]) -> dict:
    fields = {}
    for col, sources in col_map.items():
        fields[col] = {
            "inputFields": [
                {"namespace": OL_NAMESPACE, "name": table, "field": field}
                for table, field in sources
            ],
            "transformationType": "INDIRECT",
            "transformationDescription": "",
        }
    return {
        "_producer": _FACET_PRODUCER,
        "_schemaURL": _COL_LINEAGE_SCHEMA,
        "fields": fields,
    }


def _build_sql_facet(query: str) -> dict:
    return {
        "_producer": _FACET_PRODUCER,
        "_schemaURL": _SQL_SCHEMA,
        "query": query,
    }


def _send_event(event: dict) -> bool:
    body = json.dumps(event).encode()
    req = Request(
        OM_OL_ENDPOINT,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OM_JWT}",
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            edges = result.get("lineageEdgesCreated", 0)
            log.info("OpenLineage → OM: %d edge(s) criada(s)", edges)
            return edges > 0
    except Exception:
        log.exception("Falha ao enviar evento OpenLineage para OM")
        return False


def _build_event(
    inputs: list[str],
    outputs: list[str],
    job_name: str,
    job_facets: dict,
    output_facets: dict,
) -> dict:
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "run": {"runId": str(uuid.uuid4()), "facets": {}},
        "job": {"namespace": OL_NAMESPACE, "name": job_name, "facets": job_facets},
        "inputs":  [{"namespace": OL_NAMESPACE, "name": t, "facets": {}} for t in inputs],
        "outputs": [{"namespace": OL_NAMESPACE, "name": t, "facets": output_facets} for t in outputs],
        "producer": OL_PRODUCER,
        "schemaURL": OL_SCHEMA_URL,
    }


def emit_lineage(
    inputs: list[str],
    outputs: list[str],
    job_name: str = "spark",
    sql: str | None = None,
    column_lineage: dict[str, list[tuple[str, str]]] | None = None,
) -> bool:
    if not OM_JWT:
        log.warning("OM_INGESTION_BOT_JWT não definido — linhagem não enviada")
        return False
    if not inputs and not outputs:
        return False

    sql_facet = {"sql": _build_sql_facet(sql)} if sql else {}

    output_facets: dict = {}
    if column_lineage:
        output_facets["columnLineage"] = _build_column_lineage_facet(column_lineage)

    # Evento completo — registra todos os inputs + column lineage no output
    ok = _send_event(_build_event(inputs, outputs, job_name, sql_facet, output_facets))

    # Para jobs com múltiplos inputs e SQL, emite um evento por input para que
    # o OM associe o SQL à aresta individual (limitação do OM 1.12.5 com multi-input)
    if sql and len(inputs) > 1:
        for inp in inputs:
            _send_event(_build_event([inp], outputs, f"{job_name}.{inp.split('.')[-1]}", sql_facet, {}))

    return ok


def lineage_callback(context):
    """on_success_callback genérico — consulta LINEAGE_MAP pelo task_id."""
    ti = context["task_instance"]
    mapping = LINEAGE_MAP.get(ti.task_id)
    if not mapping:
        return
    job_name = f"{ti.dag_id}.{ti.task_id}"
    emit_lineage(
        inputs=mapping["inputs"],
        outputs=mapping["outputs"],
        job_name=job_name,
        sql=mapping.get("sql"),
        column_lineage=mapping.get("column_lineage"),
    )
