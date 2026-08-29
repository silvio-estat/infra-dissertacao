"""
Bronze → Silver — Spark Job (SQL puro)
Normalização, deduplicação, enriquecimento e validação de qualidade.
Usa MERGE INTO referenciando lakehouse.bronze.dados diretamente para que
o OpenLineage Spark Listener capture a linhagem automaticamente.
"""
import argparse

from pyspark.sql import SparkSession


def _comentar(spark: SparkSession, tabela: str, comentarios: dict):
    for coluna, texto in comentarios.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{texto}'")


def _comentar_tabela(spark: SparkSession, tabela: str, descricao: str):
    spark.sql(f"ALTER TABLE {tabela} SET TBLPROPERTIES ('comment' = '{descricao}')")


# Colunas de infraestrutura presentes em todas as tabelas Silver
_INFRA = {
    "batalhao_origem":    "Sigla do batalhao de origem, herdada diretamente da Bronze (ex: 1BPE, 2BIB)",
    "timestamp_geracao":  "Momento em que o dado foi gerado no batalhao (relogio do sistema de origem)",
    "timestamp_chegada":  "Momento em que o dado chegou a camada Bronze",
    "id_lote":            "Identificador do lote de ingestao Bronze",
    "latencia_ingestao_s":"Latencia de transmissao em segundos: unix(timestamp_chegada) menos unix(timestamp_geracao) — GQM Indicador 2",
    "processado_em":      "Timestamp de execucao do job Spark Silver que processou este registro",
}


def get_spark():
    return (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def criar_tabelas_silver(spark: SparkSession):
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.gps (
            id_registro         STRING,
            batalhao_origem     STRING,
            subunidade          STRING,
            latitude            DOUBLE,
            longitude           DOUBLE,
            altitude            DOUBLE,
            velocidade          DOUBLE,
            direcao             DOUBLE,
            timestamp_geracao   TIMESTAMP,
            timestamp_chegada   TIMESTAMP,
            id_lote             STRING,
            latencia_ingestao_s DOUBLE,
            fora_de_ordem       BOOLEAN,
            processado_em       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)


    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.sensor (
            id_registro         STRING,
            batalhao_origem     STRING,
            drone_id            STRING,
            area_cobertura      STRING,
            latitude_centro     DOUBLE,
            longitude_centro    DOUBLE,
            raio_km             DOUBLE,
            altitude_voo        DOUBLE,
            bateria_pct         INTEGER,
            status_missao       STRING,
            timestamp_geracao   TIMESTAMP,
            timestamp_chegada   TIMESTAMP,
            id_lote             STRING,
            latencia_ingestao_s DOUBLE,
            fora_de_ordem       BOOLEAN,
            processado_em       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)

    # --- Novas tabelas por Função de Combate ---

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.relt_intel (
            id_relatorio        STRING,
            batalhao_origem     STRING,
            subunidade          STRING,
            tipo_ameaca         STRING,
            coordenada_lat      DOUBLE,
            coordenada_lon      DOUBLE,
            efetivo_estimado    INTEGER,
            confiabilidade      STRING,
            fonte_info          STRING,
            descricao           STRING,
            timestamp_geracao   TIMESTAMP,
            timestamp_chegada   TIMESTAMP,
            id_lote             STRING,
            latencia_ingestao_s DOUBLE,
            processado_em       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.paf (
            id_paf              STRING,
            batalhao_origem     STRING,
            subunidade          STRING,
            tipo_missao         STRING,
            coordenada_alvo_lat DOUBLE,
            coordenada_alvo_lon DOUBLE,
            tipo_alvo           STRING,
            tipo_municao        STRING,
            prioridade          STRING,
            status_execucao     STRING,
            timestamp_geracao   TIMESTAMP,
            timestamp_chegada   TIMESTAMP,
            id_lote             STRING,
            latencia_ingestao_s DOUBLE,
            processado_em       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.obstaculo (
            id_obstaculo          STRING,
            batalhao_origem       STRING,
            subunidade            STRING,
            tipo_obstaculo        STRING,
            coordenada_lat        DOUBLE,
            coordenada_lon        DOUBLE,
            transitabilidade      STRING,
            coberto_fogo          BOOLEAN,
            largura_m             DOUBLE,
            confirmado_engenharia BOOLEAN,
            timestamp_geracao     TIMESTAMP,
            timestamp_chegada     TIMESTAMP,
            id_lote               STRING,
            latencia_ingestao_s   DOUBLE,
            processado_em         TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.seg_area (
            id_ocorrencia              STRING,
            batalhao_origem            STRING,
            subunidade                 STRING,
            tipo_ocorrencia            STRING,
            coordenada_lat             DOUBLE,
            coordenada_lon             DOUBLE,
            efetivo_proprio_envolvido  INTEGER,
            baixas_proprias            INTEGER,
            baixas_inimigas            INTEGER,
            nivel_ameaca               STRING,
            status_resolucao           STRING,
            timestamp_geracao          TIMESTAMP,
            timestamp_chegada          TIMESTAMP,
            id_lote                    STRING,
            latencia_ingestao_s        DOUBLE,
            processado_em              TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)

    # situacao_operacional e necessidade_logistica migrados do sitrep extinto.
    # Para resetar o schema em ambiente existente, execute antes:
    #   DROP TABLE lakehouse.silver.pessoal;
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.pessoal (
            id_relatorio             STRING,
            batalhao_origem          STRING,
            subunidade               STRING,
            situacao_operacional     STRING,
            efetivo_organico         INTEGER,
            efetivo_presente         INTEGER,
            baixas_combate           INTEGER,
            baixas_nao_combate       INTEGER,
            evacuados                INTEGER,
            necessidade_prioritaria  STRING,
            necessidade_logistica    STRING,
            timestamp_geracao        TIMESTAMP,
            timestamp_chegada        TIMESTAMP,
            id_lote                  STRING,
            latencia_ingestao_s      DOUBLE,
            processado_em            TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.material (
            id_viatura              STRING,
            batalhao_origem         STRING,
            subunidade              STRING,
            tipo_viatura            STRING,
            status_viatura          STRING,
            nivel_combustivel_pct   INTEGER,
            km_rodados              INTEGER,
            proxima_manutencao_km   INTEGER,
            timestamp_geracao       TIMESTAMP,
            timestamp_chegada       TIMESTAMP,
            id_lote                 STRING,
            latencia_ingestao_s     DOUBLE,
            fora_de_ordem           BOOLEAN,
            processado_em           TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
    """)


def migrar_schema(spark: SparkSession):
    # Adiciona baixas_inimigas em seg_area se ainda não existir (campo adicionado após criação da tabela)
    campos = [f.name for f in spark.table("lakehouse.silver.seg_area").schema.fields]
    if "baixas_inimigas" not in campos:
        spark.sql("ALTER TABLE lakehouse.silver.seg_area ADD COLUMN baixas_inimigas INTEGER")
        print("Migração: coluna baixas_inimigas adicionada em silver.seg_area")

    # Adiciona campos migrados do sitrep extinto em pessoal, se ainda não existirem
    campos_pessoal = [f.name for f in spark.table("lakehouse.silver.pessoal").schema.fields]
    if "situacao_operacional" not in campos_pessoal:
        spark.sql("ALTER TABLE lakehouse.silver.pessoal ADD COLUMN situacao_operacional STRING")
        print("Migração: coluna situacao_operacional adicionada em silver.pessoal")
    if "necessidade_logistica" not in campos_pessoal:
        spark.sql("ALTER TABLE lakehouse.silver.pessoal ADD COLUMN necessidade_logistica STRING")
        print("Migração: coluna necessidade_logistica adicionada em silver.pessoal")

    # Remove sitrep se ainda existir no Metastore — extinta, campos migrados para pessoal e seg_area
    if spark.catalog.tableExists("lakehouse.silver.sitrep"):
        spark.sql("DROP TABLE lakehouse.silver.sitrep")
        print("Migração: tabela silver.sitrep removida do Metastore")


def processar_gps(spark: SparkSession):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW gps_novos AS
        SELECT
            get_json_object(payload, '$.id_veiculo')                    AS id_registro,
            batalhao_origem,
            get_json_object(payload, '$.subunidade')                    AS subunidade,
            CAST(get_json_object(payload, '$.latitude') AS DOUBLE)      AS latitude,
            CAST(get_json_object(payload, '$.longitude') AS DOUBLE)     AS longitude,
            CAST(get_json_object(payload, '$.altitude_m') AS DOUBLE)    AS altitude,
            CAST(get_json_object(payload, '$.velocidade_kmh') AS DOUBLE) AS velocidade,
            CAST(get_json_object(payload, '$.direcao_graus') AS DOUBLE) AS direcao,
            timestamp_geracao,
            timestamp_chegada,
            id_lote,
            CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
            timestamp_geracao > timestamp_chegada                       AS fora_de_ordem,
            current_timestamp()                                         AS processado_em
        FROM lakehouse.bronze.dados
        WHERE tipo_dado = 'gps'
          AND CAST(get_json_object(payload, '$.latitude') AS DOUBLE) BETWEEN -90 AND 90
          AND CAST(get_json_object(payload, '$.longitude') AS DOUBLE) BETWEEN -180 AND 180
    """)

    spark.sql("""
        INSERT INTO lakehouse.silver.gps
        SELECT src.*
        FROM gps_novos src
        LEFT ANTI JOIN lakehouse.silver.gps tgt
            ON src.id_registro = tgt.id_registro
            AND src.timestamp_geracao = tgt.timestamp_geracao
    """)
    print(f"Silver GPS: INSERT concluído")




def processar_sensor(spark: SparkSession):
    result = spark.sql("""
        MERGE INTO lakehouse.silver.sensor AS target
        USING (
            SELECT
                get_json_object(payload, '$.id_sensor')                        AS id_registro,
                batalhao_origem,
                get_json_object(payload, '$.drone_id')                         AS drone_id,
                get_json_object(payload, '$.area_cobertura')                   AS area_cobertura,
                CAST(get_json_object(payload, '$.latitude_centro') AS DOUBLE)  AS latitude_centro,
                CAST(get_json_object(payload, '$.longitude_centro') AS DOUBLE) AS longitude_centro,
                CAST(get_json_object(payload, '$.raio_km') AS DOUBLE)          AS raio_km,
                CAST(get_json_object(payload, '$.altitude_voo') AS DOUBLE)     AS altitude_voo,
                CAST(get_json_object(payload, '$.bateria_pct') AS INTEGER)     AS bateria_pct,
                get_json_object(payload, '$.status_missao')                    AS status_missao,
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
                timestamp_geracao > timestamp_chegada                       AS fora_de_ordem,
                current_timestamp()                                         AS processado_em
            FROM lakehouse.bronze.dados
            WHERE tipo_dado = 'sensor'
        ) AS source
        ON target.id_registro = source.id_registro
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Silver Sensor: MERGE concluído")


def processar_relt_intel(spark: SparkSession):
    spark.sql("""
        MERGE INTO lakehouse.silver.relt_intel AS target
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
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
                current_timestamp()                                             AS processado_em
            FROM lakehouse.bronze.dados
            WHERE tipo_dado = 'relt_intel'
        ) AS source
        ON target.id_relatorio = source.id_relatorio
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Silver relt_intel: MERGE concluído")


def processar_paf(spark: SparkSession):
    spark.sql("""
        MERGE INTO lakehouse.silver.paf AS target
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
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
                current_timestamp()                                               AS processado_em
            FROM lakehouse.bronze.dados
            WHERE tipo_dado = 'paf'
        ) AS source
        ON target.id_paf = source.id_paf
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Silver paf: MERGE concluído")


def processar_obstaculo(spark: SparkSession):
    spark.sql("""
        MERGE INTO lakehouse.silver.obstaculo AS target
        USING (
            SELECT
                get_json_object(payload, '$.id_obstaculo')                          AS id_obstaculo,
                batalhao_origem,
                get_json_object(payload, '$.subunidade')                            AS subunidade,
                get_json_object(payload, '$.tipo_obstaculo')                        AS tipo_obstaculo,
                CAST(get_json_object(payload, '$.coordenada_lat') AS DOUBLE)        AS coordenada_lat,
                CAST(get_json_object(payload, '$.coordenada_lon') AS DOUBLE)        AS coordenada_lon,
                get_json_object(payload, '$.transitabilidade')                      AS transitabilidade,
                CAST(get_json_object(payload, '$.coberto_fogo') AS BOOLEAN)         AS coberto_fogo,
                CAST(get_json_object(payload, '$.largura_m') AS DOUBLE)             AS largura_m,
                CAST(get_json_object(payload, '$.confirmado_engenharia') AS BOOLEAN) AS confirmado_engenharia,
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
                current_timestamp()                                                 AS processado_em
            FROM lakehouse.bronze.dados
            WHERE tipo_dado = 'obstaculo'
        ) AS source
        ON target.id_obstaculo = source.id_obstaculo
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Silver obstaculo: MERGE concluído")


def processar_seg_area(spark: SparkSession):
    spark.sql("""
        MERGE INTO lakehouse.silver.seg_area AS target
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
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
                current_timestamp()                                                      AS processado_em
            FROM lakehouse.bronze.dados
            WHERE tipo_dado = 'seg_area'
        ) AS source
        ON target.id_ocorrencia = source.id_ocorrencia
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Silver seg_area: MERGE concluído")


def processar_pessoal(spark: SparkSession):
    spark.sql("""
        MERGE INTO lakehouse.silver.pessoal AS target
        USING (
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
                timestamp_geracao,
                timestamp_chegada,
                id_lote,
                CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
                current_timestamp() AS processado_em
            FROM lakehouse.bronze.dados
            WHERE tipo_dado = 'pessoal'
        ) AS source
        ON target.id_relatorio = source.id_relatorio
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Silver pessoal: MERGE concluído")


def processar_material(spark: SparkSession):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW material_novos AS
        SELECT
            get_json_object(payload, '$.id_viatura')                               AS id_viatura,
            batalhao_origem,
            get_json_object(payload, '$.subunidade')                               AS subunidade,
            get_json_object(payload, '$.tipo_viatura')                             AS tipo_viatura,
            get_json_object(payload, '$.status_viatura')                           AS status_viatura,
            CAST(get_json_object(payload, '$.nivel_combustivel_pct') AS INTEGER)   AS nivel_combustivel_pct,
            CAST(get_json_object(payload, '$.km_rodados') AS INTEGER)              AS km_rodados,
            CAST(get_json_object(payload, '$.proxima_manutencao_km') AS INTEGER)   AS proxima_manutencao_km,
            timestamp_geracao,
            timestamp_chegada,
            id_lote,
            CAST(unix_timestamp(timestamp_chegada) - unix_timestamp(timestamp_geracao) AS DOUBLE) AS latencia_ingestao_s,
            timestamp_geracao > timestamp_chegada AS fora_de_ordem,
            current_timestamp() AS processado_em
        FROM lakehouse.bronze.dados
        WHERE tipo_dado = 'material'
    """)

    spark.sql("""
        INSERT INTO lakehouse.silver.material
        SELECT src.*
        FROM material_novos src
        LEFT ANTI JOIN lakehouse.silver.material tgt
            ON src.id_viatura        = tgt.id_viatura
           AND src.timestamp_geracao = tgt.timestamp_geracao
    """)
    print("Silver material: INSERT concluído")


def comentar_silver(spark: SparkSession):
    _comentar_tabela(spark, "lakehouse.silver.gps",
        "Posicionamento das viaturas proprias — Funcao de Combate Manobra. "
        "Normalizado, validado (coordenadas WGS-84) e deduplicado por (id_registro, timestamp_geracao) "
        "via LEFT ANTI JOIN. Particionado por dia de chegada e batalhao_origem."
    )
    _comentar(spark, "lakehouse.silver.gps", {
        **_INFRA,
        "id_registro":   "Identificador do veiculo extraido de id_veiculo (formato VTR-<bat>-<sub_idx>-<seq>) — permite rastrear historico de posicao",
        "subunidade":    "Subunidade organica do veiculo (ex: 1a Cia PE, Cia Cmdo)",
        "latitude":      "Latitude em graus decimais WGS-84, validada entre -90 e 90",
        "longitude":     "Longitude em graus decimais WGS-84, validada entre -180 e 180",
        "altitude":      "Altitude em metros acima do nivel do mar",
        "velocidade":    "Velocidade instantanea do veiculo em km/h",
        "direcao":       "Rumo magnetico em graus (0 a 359)",
        "fora_de_ordem": "TRUE quando timestamp_geracao maior que timestamp_chegada — indica relogio dessincronizado no sistema de origem",
    })
    _comentar_tabela(spark, "lakehouse.silver.sensor",
        "Dados de reconhecimento aereo por drones — Funcao de Combate Inteligencia (vigilancia continua). "
        "Deduplicado por id_sensor via MERGE INTO. Particionado por dia de chegada e batalhao_origem."
    )
    _comentar(spark, "lakehouse.silver.sensor", {
        **_INFRA,
        "id_registro":      "Identificador do sensor extraido de id_sensor (mesmo valor de drone_id)",
        "drone_id":         "Identificador do drone (formato DRN-<bat>-<seq>)",
        "area_cobertura":   "Setor geografico monitorado: NORTE, SUL, LESTE, OESTE, CENTRO",
        "latitude_centro":  "Latitude do centro da area de cobertura do drone (WGS-84)",
        "longitude_centro": "Longitude do centro da area de cobertura do drone (WGS-84)",
        "raio_km":          "Raio da area monitorada pelo drone em quilometros",
        "altitude_voo":     "Altitude de voo do drone em metros",
        "bateria_pct":      "Nivel de bateria do drone (0 a 100%) — determina autonomia remanescente da missao",
        "status_missao":    "Estado atual: ativo (em operacao), retornando (fim de missao), em_espera (aguardando), manutencao (indisponivel)",
        "fora_de_ordem":    "TRUE quando timestamp_geracao maior que timestamp_chegada",
    })
    _comentar_tabela(spark, "lakehouse.silver.relt_intel",
        "Relatorios de avistamento e identificacao de ameacas inimigas — Funcao de Combate Inteligencia. "
        "Confiabilidade segue escala doutrinaria combinada: letra (A=confiavel, B=geralmente confiavel, C=nao testada) "
        "mais numero (1=confirmada, 2=provavel, 3=possivel). Deduplicado por id_relatorio via MERGE INTO."
    )
    _comentar(spark, "lakehouse.silver.relt_intel", {
        **_INFRA,
        "id_relatorio":       "Identificador unico do relatorio de inteligencia (UUID gerado no batalhao)",
        "subunidade":         "Subunidade que obteve ou coletou a informacao de inteligencia",
        "tipo_ameaca":        "Categoria do elemento inimigo: TROPA_PE, BLINDADO, ARTILHARIA, FRANCO_ATIRADOR, DRONE_INIMIGO, VEICULO_SUSPEITO",
        "coordenada_lat":     "Latitude da posicao estimada da ameaca (WGS-84)",
        "coordenada_lon":     "Longitude da posicao estimada da ameaca (WGS-84)",
        "efetivo_estimado":   "Numero de militares inimigos estimados no avistamento",
        "confiabilidade":     "Avaliacao combinada fonte+informacao: letra (A=confiavel, B=geralmente confiavel, C=nao testada) + numero (1=confirmada, 2=provavel, 3=possivel)",
        "fonte_info":         "Origem da informacao: OBSERVACAO_DIRETA, CAPTURADO, AGENTE, SENSOR_DRONE, RELATO_CIVIL",
        "descricao":          "Texto livre com detalhes qualitativos do avistamento",
    })
    _comentar_tabela(spark, "lakehouse.silver.paf",
        "Pedidos de Apoio de Fogo — Funcao de Combate Fogos. "
        "Pipeline de fogos rastreado pelo campo status_execucao: SOLICITADO, APROVADO, EXECUTADO, CANCELADO. "
        "Deduplicado por id_paf via MERGE INTO."
    )
    _comentar(spark, "lakehouse.silver.paf", {
        **_INFRA,
        "id_paf":              "Identificador unico do Pedido de Apoio de Fogo (UUID)",
        "subunidade":          "Subunidade que originou o pedido de apoio de fogo",
        "tipo_missao":         "Natureza tatica: SUPORTE_IMEDIATO, SUPORTE_GERAL, CONTRABATERIA, SUPRESSAO",
        "coordenada_alvo_lat": "Latitude do alvo a ser batido (WGS-84)",
        "coordenada_alvo_lon": "Longitude do alvo a ser batido (WGS-84)",
        "tipo_alvo":           "Categoria do alvo: PESSOAL_DESCOBERTO, VEICULO, POSICAO_DEFENSIVA, MATERIAL, AREA_SUSPEITA",
        "tipo_municao":        "Municao solicitada: EXPLOSIVO, FUMACA (mascaramento), ILUMINACAO (visibilidade noturna)",
        "prioridade":          "Urgencia do pedido: URGENTE, PRIORITARIO, ROTINA",
        "status_execucao":     "Estado atual: SOLICITADO (aguardando aprovacao), APROVADO (autorizado), EXECUTADO (concluido), CANCELADO",
    })
    _comentar_tabela(spark, "lakehouse.silver.obstaculo",
        "Obstaculos identificados no terreno — Funcao de Combate Manobra/Protecao. "
        "Insumo para analise de transitabilidade propria e contra-mobilidade defensiva. "
        "Deduplicado por id_obstaculo via MERGE INTO."
    )
    _comentar(spark, "lakehouse.silver.obstaculo", {
        **_INFRA,
        "id_obstaculo":          "Identificador unico do obstaculo (UUID)",
        "subunidade":            "Subunidade que realizou o reconhecimento e reportou o obstaculo",
        "tipo_obstaculo":        "Natureza fisica: MINA, BARREIRA_FISICA, INUNDACAO, DESTRUICAO_PONTE, ENTULHO, AREA_CONTAMINADA",
        "coordenada_lat":        "Latitude da posicao do obstaculo (WGS-84)",
        "coordenada_lon":        "Longitude da posicao do obstaculo (WGS-84)",
        "transitabilidade":      "Impacto a mobilidade: INTRANSITAVEL (passagem impossivel), RESTRITO (passagem reduzida), TRANSITAVEL (pouco impacto)",
        "coberto_fogo":          "TRUE quando o obstaculo e coberto por fogo inimigo — aumenta o custo tatico de franqueamento",
        "largura_m":             "Dimensao do obstaculo perpendicular ao eixo de progressao em metros",
        "confirmado_engenharia": "TRUE quando a tropa de Engenharia confirmou presenca e natureza em reconhecimento proprio",
    })
    _comentar_tabela(spark, "lakehouse.silver.seg_area",
        "Ocorrencias de seguranca de area — Funcao de Combate Protecao. "
        "Registra incidentes com forcas proprias (IED, emboscada, infiltracao, etc.) "
        "com baixas proprias e inimigas. Deduplicado por id_ocorrencia via MERGE INTO."
    )
    _comentar(spark, "lakehouse.silver.seg_area", {
        **_INFRA,
        "id_ocorrencia":             "Identificador unico da ocorrencia de seguranca de area (UUID)",
        "subunidade":                "Subunidade diretamente envolvida no incidente",
        "tipo_ocorrencia":           "Categoria do incidente: INFILTRACAO, ATAQUE_SNIPER, IED, EMBOSCADA, ATIVIDADE_SUSPEITA, VIOLACAO_PERIMETRO",
        "coordenada_lat":            "Latitude do local do incidente (WGS-84)",
        "coordenada_lon":            "Longitude do local do incidente (WGS-84)",
        "efetivo_proprio_envolvido": "Numero de militares proprios diretamente envolvidos na ocorrencia",
        "baixas_proprias":           "Baixas sofridas pelas forcas proprias no incidente",
        "baixas_inimigas":           "Baixas confirmadas do inimigo no incidente",
        "nivel_ameaca":              "Grau de criticidade: BAIXO, MEDIO, ALTO, CRITICO",
        "status_resolucao":          "Estado de resolucao: EM_ANDAMENTO (em curso), RESOLVIDO (encerrado), PENDENTE (aguardando providencias)",
    })
    _comentar_tabela(spark, "lakehouse.silver.pessoal",
        "Relatorios de efetivo por subunidade — Funcao de Combate Logistica (S1). "
        "Inclui situacao operacional declarada e necessidade logistica prioritaria (S4). "
        "Granularidade: uma linha por relatorio de subunidade. Deduplicado por id_relatorio via LEFT ANTI JOIN."
    )
    _comentar(spark, "lakehouse.silver.pessoal", {
        **_INFRA,
        "id_relatorio":            "Identificador unico do relatorio de pessoal (UUID gerado no batalhao)",
        "subunidade":              "Subunidade reportante (granularidade do relatorio S1)",
        "situacao_operacional":    "Prontidao operacional declarada pelo S1: OPERACIONAL, DEGRADADO, INOPERANTE, RESERVA",
        "efetivo_organico":        "Total de militares previsto no quadro organico da subunidade (fixo em 120 na PoC)",
        "efetivo_presente":        "Total de militares prestos ao servico no momento do relatorio",
        "baixas_combate":          "Militares mortos ou feridos em acao de combate direta",
        "baixas_nao_combate":      "Militares perdidos por acidentes, doencas ou outras causas nao relacionadas ao combate",
        "evacuados":               "Militares afastados para atendimento medico externo (podem retornar)",
        "necessidade_prioritaria": "Necessidade S1 mais urgente: PESSOAL_REFORCADO, EVACUACAO_MEDICA, NENHUMA",
        "necessidade_logistica":   "Necessidade S4 declarada pela subunidade: MUNICAO, COMBUSTIVEL, RACOES, MATERIAL SAUDE, PECAS REPOSICAO, AGUA, BATERIAS, NENHUMA",
    })
    _comentar_tabela(spark, "lakehouse.silver.material",
        "Estado de cada viatura individualmente — Funcao de Combate Logistica (S4). "
        "Granularidade: uma linha por viatura por reporte, permitindo rastrear historico "
        "de status operacional, combustivel e quilometragem. Deduplicado por (id_viatura, timestamp_geracao) via LEFT ANTI JOIN."
    )
    _comentar(spark, "lakehouse.silver.material", {
        **_INFRA,
        "id_viatura":            "Identificador unico da viatura (formato VTR-<bat>-<sub_idx>-<seq>) — chave de identidade ao longo do tempo",
        "subunidade":            "Subunidade organica a qual a viatura pertence",
        "tipo_viatura":          "Categoria do veiculo: VBTP, VTR_CARGA, VTR_CMDO, AMBULANCIA, VTR_MANT",
        "status_viatura":        "Estado operacional: OPERACIONAL, MANUTENCAO, BAIXADO_TECNICO, BAIXADO_COMBATE",
        "nivel_combustivel_pct": "Nivel de combustivel em percentual (0 a 100%)",
        "km_rodados":            "Quilometragem total acumulada da viatura",
        "proxima_manutencao_km": "Quilometragem programada para a proxima manutencao preventiva",
        "fora_de_ordem":         "TRUE quando timestamp_geracao maior que timestamp_chegada",
    })


_PROCESSADORES = {
    "gps":        processar_gps,
    "sensor":     processar_sensor,
    "relt_intel": processar_relt_intel,
    "paf":        processar_paf,
    "obstaculo":  processar_obstaculo,
    "seg_area":   processar_seg_area,
    "pessoal":    processar_pessoal,
    "material":   processar_material,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tipo", choices=list(_PROCESSADORES.keys()), required=True)
    args = parser.parse_args()

    spark = get_spark()
    criar_tabelas_silver(spark)
    migrar_schema(spark)
    comentar_silver(spark)
    _PROCESSADORES[args.tipo](spark)
    spark.stop()


if __name__ == "__main__":
    main()
