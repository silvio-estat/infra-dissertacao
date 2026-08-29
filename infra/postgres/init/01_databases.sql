-- Inicialização do PostgreSQL: cria os bancos de dados do projeto

-- Hive Metastore (catálogo Iceberg)
CREATE DATABASE metastore_db;

-- Apache Airflow — orquestração Lakehouse
CREATE DATABASE airflow_db;

-- Baseline relacional (paradigma relacional para comparação com Lakehouse)
CREATE DATABASE baseline_db;

-- OpenMetadata — catálogo de governança
CREATE DATABASE openmetadata_db;

-- OpenMetadata Ingestion — Airflow interno do OpenMetadata
CREATE DATABASE airflow_om_db;

-- Permissões ao usuário padrão
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO dlh_admin;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO dlh_admin;
GRANT ALL PRIVILEGES ON DATABASE baseline_db TO dlh_admin;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO dlh_admin;
GRANT ALL PRIVILEGES ON DATABASE airflow_om_db TO dlh_admin;

-- ============================================================
-- Schema do baseline relacional (paradigma relacional)
-- Engenharia equivalente ao Lakehouse conforme seção 5.2 do relatório
-- ============================================================
\c baseline_db

CREATE TABLE IF NOT EXISTS gps_posicionamento (
    id_registro     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batalhao_origem VARCHAR(20) NOT NULL,
    subunidade      VARCHAR(50),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    altitude        DOUBLE PRECISION,
    velocidade      DOUBLE PRECISION,
    direcao         DOUBLE PRECISION,
    timestamp_geracao   TIMESTAMPTZ NOT NULL,
    timestamp_chegada   TIMESTAMPTZ NOT NULL,
    id_lote         VARCHAR(50),
    criado_em       TIMESTAMPTZ DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS sensor_drone (
    id_registro         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batalhao_origem     VARCHAR(20) NOT NULL,
    drone_id            VARCHAR(50),
    area_cobertura      TEXT,
    latitude_centro     DOUBLE PRECISION,
    longitude_centro    DOUBLE PRECISION,
    raio_km             DOUBLE PRECISION,
    altitude_voo        DOUBLE PRECISION,
    bateria_pct         INTEGER,
    status_missao       VARCHAR(30),
    timestamp_geracao   TIMESTAMPTZ NOT NULL,
    timestamp_chegada   TIMESTAMPTZ NOT NULL,
    id_lote             VARCHAR(50),
    criado_em           TIMESTAMPTZ DEFAULT NOW()
);

-- Índices otimizados (engenharia equivalente ao Lakehouse)
CREATE INDEX idx_gps_batalhao_ts ON gps_posicionamento(batalhao_origem, timestamp_geracao DESC);
CREATE INDEX idx_gps_ts_chegada  ON gps_posicionamento(timestamp_chegada DESC);
CREATE INDEX idx_sensor_batalhao_ts ON sensor_drone(batalhao_origem, timestamp_geracao DESC);

-- Tabela Logística (S1) — Efetivo por subunidade (espelho de silver.pessoal)
CREATE TABLE IF NOT EXISTS pessoal_subunidade (
    id_registro             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_relatorio            VARCHAR(36) UNIQUE NOT NULL,
    batalhao_origem         VARCHAR(20) NOT NULL,
    subunidade              VARCHAR(50),
    situacao_operacional    VARCHAR(20),
    efetivo_organico        INTEGER,
    efetivo_presente        INTEGER,
    baixas_combate          INTEGER DEFAULT 0,
    baixas_nao_combate      INTEGER DEFAULT 0,
    evacuados               INTEGER DEFAULT 0,
    necessidade_prioritaria VARCHAR(30),
    necessidade_logistica   VARCHAR(30),
    timestamp_geracao       TIMESTAMPTZ NOT NULL,
    timestamp_chegada       TIMESTAMPTZ NOT NULL,
    id_lote                 VARCHAR(50),
    criado_em               TIMESTAMPTZ DEFAULT NOW()
);

-- Tabela Logística (S4) — Estado do material por viatura (espelho de silver.material)
CREATE TABLE IF NOT EXISTS material_viatura (
    id_registro           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_viatura            VARCHAR(30) NOT NULL,
    batalhao_origem       VARCHAR(20) NOT NULL,
    subunidade            VARCHAR(50),
    tipo_viatura          VARCHAR(20),
    status_viatura        VARCHAR(25),
    nivel_combustivel_pct INTEGER,
    km_rodados            INTEGER,
    proxima_manutencao_km INTEGER,
    timestamp_geracao     TIMESTAMPTZ NOT NULL,
    timestamp_chegada     TIMESTAMPTZ NOT NULL,
    id_lote               VARCHAR(50),
    criado_em             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_pessoal_batalhao_ts ON pessoal_subunidade(batalhao_origem, timestamp_geracao DESC);
CREATE INDEX idx_material_viatura_ts ON material_viatura(id_viatura, timestamp_geracao DESC);
CREATE INDEX idx_material_batalhao   ON material_viatura(batalhao_origem, subunidade);

-- View consolidada pessoal + material por subunidade (equivalente ao gold.coc S1+S4)
CREATE OR REPLACE VIEW v_estado_forca AS
WITH ult_pessoal AS (
    SELECT DISTINCT ON (batalhao_origem, subunidade)
        batalhao_origem, subunidade,
        efetivo_organico, efetivo_presente,
        ROUND(efetivo_presente * 100.0 / NULLIF(efetivo_organico, 0), 1) AS pct_efetivo,
        baixas_combate, baixas_nao_combate, evacuados,
        necessidade_prioritaria,
        timestamp_geracao AS ts_pessoal
    FROM pessoal_subunidade
    ORDER BY batalhao_origem, subunidade, timestamp_geracao DESC
),
ult_material AS (
    SELECT batalhao_origem, subunidade,
           COUNT(DISTINCT id_viatura) AS viaturas_total,
           SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) AS viaturas_operacionais,
           ROUND(SUM(CASE WHEN status_viatura = 'OPERACIONAL' THEN 1 ELSE 0 END) * 100.0
                 / NULLIF(COUNT(DISTINCT id_viatura), 0), 1) AS pct_viaturas,
           ROUND(AVG(nivel_combustivel_pct), 1) AS combustivel_medio_pct
    FROM (
        SELECT DISTINCT ON (id_viatura)
            batalhao_origem, subunidade, id_viatura,
            status_viatura, nivel_combustivel_pct
        FROM material_viatura
        ORDER BY id_viatura, timestamp_geracao DESC
    ) ultima
    GROUP BY batalhao_origem, subunidade
)
SELECT
    p.batalhao_origem, p.subunidade,
    p.efetivo_organico, p.efetivo_presente, p.pct_efetivo,
    p.baixas_combate, p.baixas_nao_combate, p.evacuados,
    p.necessidade_prioritaria, p.ts_pessoal,
    m.viaturas_total, m.viaturas_operacionais,
    m.pct_viaturas, m.combustivel_medio_pct
FROM ult_pessoal p
LEFT JOIN ult_material m
    ON p.batalhao_origem = m.batalhao_origem
   AND p.subunidade      = m.subunidade;

-- Views analíticas equivalentes às visões Gold do Lakehouse (seção 5.2)
CREATE OR REPLACE VIEW v_posicionamento_atual AS
SELECT DISTINCT ON (batalhao_origem, subunidade)
    batalhao_origem, subunidade, latitude, longitude,
    altitude, velocidade, direcao, timestamp_geracao,
    timestamp_chegada, id_lote
FROM gps_posicionamento
ORDER BY batalhao_origem, subunidade, timestamp_geracao DESC;

CREATE OR REPLACE VIEW v_latencia_por_batalhao AS
SELECT
    batalhao_origem,
    AVG(EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS latencia_media_s,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS p90,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (timestamp_chegada - timestamp_geracao))) AS p99,
    COUNT(*) AS total_registros
FROM (
    SELECT batalhao_origem, timestamp_geracao, timestamp_chegada FROM gps_posicionamento
    UNION ALL
    SELECT batalhao_origem, timestamp_geracao, timestamp_chegada FROM pessoal_subunidade
    UNION ALL
    SELECT batalhao_origem, timestamp_geracao, timestamp_chegada FROM sensor_drone
) dados
GROUP BY batalhao_origem;
