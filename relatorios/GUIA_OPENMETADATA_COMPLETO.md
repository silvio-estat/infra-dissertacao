# Guia Completo: Como o OpenMetadata Funciona Neste Projeto

> **Objetivo:** Este documento explica, passo a passo, como cada funcionalidade do
> OpenMetadata (lineage, sample data, data observability) foi implementada neste projeto.
> Se você está tentando reproduzir os resultados em outra máquina, siga este guia.
> **NÃO MEXA NAS DAGs DO PIPELINE (dag_ingestao_bronze, dag_silver_transform,
> dag_gold_refresh, dag_iceberg_maintenance) — elas já estão funcionando.**

---

## Índice

1. [Visão Geral da Arquitetura OM](#1-visão-geral-da-arquitetura-om)
2. [Serviços Docker do OpenMetadata](#2-serviços-docker-do-openmetadata)
3. [Pré-requisitos: Pipeline Lakehouse Funcionando](#3-pré-requisitos-pipeline-lakehouse-funcionando)
4. [Feature 1: Metadata Ingestion (Descoberta de Tabelas)](#4-feature-1-metadata-ingestion)
5. [Feature 2: Lineage (Linhagem de Dados)](#5-feature-2-lineage)
6. [Feature 3: Sample Data (Amostras de Dados)](#6-feature-3-sample-data)
7. [Feature 4: Data Observability (Profiling/Estatísticas)](#7-feature-4-data-observability)
8. [Ordem de Execução Correta](#8-ordem-de-execução-correta)
9. [Autenticação e JWT](#9-autenticação-e-jwt)
10. [Troubleshooting](#10-troubleshooting)
11. [Mapa Completo de Arquivos](#11-mapa-completo-de-arquivos)

---

## 1. Visão Geral da Arquitetura OM

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DOCKER NETWORK (dlh_net)                     │
│                                                                     │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────────┐   │
│  │  Spark   │───>│  OpenLineage │───>│   OpenMetadata Server    │   │
│  │ Master/  │    │  Listener    │    │   (porta 8585)           │   │
│  │ Worker   │    │  (JAR 1.18)  │    │                          │   │
│  └──────────┘    └──────────────┘    │  ┌────────────────────┐  │   │
│       │                         HTTP │  │  Elasticsearch     │  │   │
│       │ MERGE INTO              POST │  │  (busca/índice)    │  │   │
│       ▼                              │  └────────────────────┘  │   │
│  ┌──────────┐                        │  ┌────────────────────┐  │   │
│  │  Iceberg │                        │  │  PostgreSQL        │  │   │
│  │  Tables  │◄──── Trino ──────────> │  │  (openmetadata_db) │  │   │
│  │  (MinIO) │     query              │  └────────────────────┘  │   │
│  └──────────┘                        └──────────────────────────┘   │
│       ▲                                          ▲                  │
│       │                                          │                  │
│  ┌──────────────────────────────────────────┐    │                  │
│  │         Airflow (nosso)                   │    │                  │
│  │                                           │    │                  │
│  │  dag_silver_transform ──on_success──────>─┤    │                  │
│  │  dag_gold_refresh ──────on_success──────>─┤    │                  │
│  │      (lineage_emitter.py)                 │    │ REST API         │
│  │                                           │    │                  │
│  │  trino_lakehouse_metadata ─── SDK OM ────>┘    │                  │
│  │  trino_lakehouse_profiler ─── SDK OM ────>┘    │                  │
│  │  trino_sample_data_collector ── REST ────>┘    │                  │
│  └──────────────────────────────────────────┘    │                  │
│                                                   │                  │
│  ┌──────────────────────────────────────────┐    │                  │
│  │  inject_lineage.py (manual, uma vez) ────>────┘                  │
│  └──────────────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────┘
```

**Fluxo resumido:**
1. O pipeline Lakehouse (Bronze→Silver→Gold) roda normalmente via Spark
2. Cada DAG dispara um callback `lineage_callback` ao concluir com sucesso → envia eventos OpenLineage para o OM
3. DAGs auxiliares (metadata, profiler, sample data) são disparadas manualmente para popular o OM
4. O script `inject_lineage.py` pode ser usado como bootstrap para injetar toda a linhagem de uma vez

---

## 2. Serviços Docker do OpenMetadata

Há **4 containers** dedicados ao OM, definidos em `docker-compose.yml` (linhas ~295-389):

### 2.1 Elasticsearch

```yaml
# Container: dlh_elasticsearch
# Imagem: docker.elastic.co/elasticsearch/elasticsearch:9.3.0
# Porta: 9200
```
- **O que faz:** Índice de busca full-text para o OM. Permite pesquisar tabelas, colunas, pipelines pela UI.
- **Configuração:** Single-node, segurança desabilitada, heap 512MB.
- **Volume:** `elasticsearch_data` (persistente).

### 2.2 Execute-Migrate-All (one-shot)

```yaml
# Container: dlh_openmetadata_migrate
# Imagem: docker.getcollate.io/openmetadata/server:1.12.5
```
- **O que faz:** Roda UMA VEZ ao subir o stack. Executa `./bootstrap/openmetadata-ops.sh migrate` para criar o schema do OM no PostgreSQL (banco `openmetadata_db`).
- **Depende de:** postgres + elasticsearch healthy.
- **Depois de rodar:** Para sozinho (`restart: on-failure`).

### 2.3 OpenMetadata Server

```yaml
# Container: dlh_openmetadata
# Imagem: docker.getcollate.io/openmetadata/server:1.12.5
# Portas: 8585 (API/UI), 8586 (admin)
```
- **O que faz:** Servidor principal. Hospeda a UI web, a API REST, recebe eventos OpenLineage.
- **Banco:** PostgreSQL (`openmetadata_db`), compartilha o mesmo container `dlh_postgres`.
- **Variáveis críticas:**
  - `PIPELINE_SERVICE_CLIENT_ENDPOINT: http://openmetadata-ingestion:8080` — aponta para o Airflow interno do OM
  - `SERVER_HOST_API_URL: http://openmetadata:8585/api` — URL pública da API
  - `SEARCH_TYPE: elasticsearch`
- **Depende de:** migrate ter completado com sucesso.

### 2.4 OpenMetadata Ingestion

```yaml
# Container: dlh_openmetadata_ingestion
# Imagem: docker.getcollate.io/openmetadata/ingestion:1.12.5
# Porta: 8080 (Airflow interno do OM)
```
- **O que faz:** Roda um Airflow INTERNO ao OM para executar pipelines de ingestão (metadata, profiler). Este Airflow é DIFERENTE do nosso Airflow principal (`dlh_airflow_webserver`).
- **Banco:** PostgreSQL (`airflow_om_db`), outro banco no mesmo container Postgres.
- **IMPORTANTE:** Neste projeto, NÃO usamos o Airflow interno do OM. As DAGs de ingestão rodam no **nosso** Airflow (`dlh_airflow_webserver`), porque ele tem o SDK `openmetadata-ingestion` instalado e acesso ao Trino.

---

## 3. Pré-requisitos: Pipeline Lakehouse Funcionando

Antes de configurar qualquer coisa no OpenMetadata, o pipeline de dados precisa estar
rodando e com dados nas tabelas. Verifique:

```bash
# 1. Todos os containers UP
docker compose ps

# 2. Dados existem no Bronze
docker exec dlh_trino trino --execute "SELECT count(*) FROM iceberg.bronze.dados"
# Deve retornar > 0

# 3. Dados existem no Silver
docker exec dlh_trino trino --execute "SELECT count(*) FROM iceberg.silver.gps"
docker exec dlh_trino trino --execute "SELECT count(*) FROM iceberg.silver.sitrep"
docker exec dlh_trino trino --execute "SELECT count(*) FROM iceberg.silver.sensor"

# 4. Dados existem no Gold
docker exec dlh_trino trino --execute "SELECT count(*) FROM iceberg.gold.posicionamento_atual"
```

**Se as tabelas não existem ou estão vazias:** rode as DAGs do pipeline primeiro
(`dag_ingestao_bronze` → `dag_silver_transform` → `dag_gold_refresh`).
Essas DAGs NÃO precisam ser modificadas.

---

## 4. Feature 1: Metadata Ingestion

### O que é
O OM precisa "descobrir" quais tabelas existem no seu data lake. A ingestão de metadados
lê o catálogo Trino/Iceberg e registra cada tabela, suas colunas, tipos e schemas no OM.

### Como funciona

**Arquivo:** `airflow/dags/trino_lakehouse_metadata.py`

Esta DAG usa o SDK do OpenMetadata (`metadata.workflow.metadata.MetadataWorkflow`) para:
1. Conectar ao Trino (`trino:8090`, catálogo `iceberg`)
2. Listar todos os schemas e tabelas (bronze, silver, gold)
3. Extrair colunas, tipos de dados, constraints
4. Enviar tudo via REST para `http://openmetadata:8585/api`

**Configuração inline (dentro da DAG):**
```yaml
source:
  type: trino
  serviceName: trino_lakehouse    # ← este é o nome do serviço no OM
  sourceConfig:
    config:
      type: DatabaseMetadata
processor:
  type: orm-metadata
sink:
  type: metadata-rest
workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata:8585/api"
    authProvider: openmetadata     # ← autentica automaticamente via SDK
```

### Pré-requisito no OM
Antes de rodar esta DAG, o serviço `trino_lakehouse` deve existir no OM.
Crie manualmente pela UI:
1. Acesse `http://localhost:8585`
2. Login: `admin` / `admin`
3. Menu → Settings → Database Services → Add New Service
4. Tipo: **Trino**
5. Nome: **trino_lakehouse**
6. Host: `trino` (nome do container na rede Docker)
7. Port: `8090`
8. Username: `trino`
9. Catalog: `iceberg`
10. Salvar

### Como executar
```bash
docker exec dlh_airflow_webserver airflow dags trigger trino_lakehouse_metadata
```

### Resultado esperado
Na UI do OM: Database Services → trino_lakehouse → você verá os schemas `bronze`, `silver`, `gold` com todas as tabelas e colunas.

---

## 5. Feature 2: Lineage (Linhagem de Dados)

A linhagem mostra o fluxo dos dados: Bronze → Silver → Gold, incluindo quais colunas
alimentam quais outras colunas (column lineage) e o SQL usado na transformação.

### EXISTEM 3 MECANISMOS (complementares, não excludentes)

### 5.1 Mecanismo A: OpenLineage Spark Listener (automático)

**O que é:** Um JAR (`openlineage-spark_2.12-1.18.0.jar`) instalado no Spark que
intercepta automaticamente operações MERGE INTO e emite eventos OpenLineage.

**Como está configurado:**

1. **Dockerfile do Spark** (`infra/spark/Dockerfile`, linha 17):
   ```dockerfile
   RUN curl -fsSL -o /opt/spark/jars/openlineage-spark_2.12-1.18.0.jar \
       https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.18.0/...
   ```

2. **spark-defaults.conf** (`infra/spark/spark-defaults.conf`, linha 28):
   ```properties
   spark.extraListeners  io.openlineage.spark.agent.OpenLineageSparkListener
   ```

3. **Configuração de transporte** (`infra/openlineage/openlineage.yml`):
   ```yaml
   transport:
     type: http
     url: http://openmetadata:8585
     endpoint: /api/v1/openlineage/lineage
     auth:
       type: api_key
       apiKey: <JWT do ingestion-bot>
   ```

4. **Bind mount nos containers** (`docker-compose.yml`):
   ```yaml
   # spark-master e spark-worker:
   volumes:
     - ./infra/openlineage/openlineage.yml:/opt/openlineage/openlineage.yml:ro
   environment:
     OPENLINEAGE_CONFIG: /opt/openlineage/openlineage.yml
   ```

**Limitação:** O listener emite datasets com namespace derivado do Hive Metastore
(`hive://hive-metastore:9083`), que o OM 1.12.5 NÃO consegue resolver para o serviço
`trino_lakehouse`. Por isso criamos o Mecanismo B.

### 5.2 Mecanismo B: lineage_emitter.py (callback dos DAGs)

**Arquivo:** `airflow/dags/helpers/lineage_emitter.py`

Este é o mecanismo PRINCIPAL de linhagem que realmente funciona. Ele:
1. Define um `LINEAGE_MAP` centralizado com TODAS as relações de linhagem
2. É disparado automaticamente como `on_success_callback` nos DAGs do pipeline
3. Emite eventos OpenLineage com namespace `dlh` (que o OM consegue resolver)
4. Inclui facets de SQL (query completa) e column lineage (mapeamento coluna-a-coluna)

**Como é acionado** (nos DAGs `dag_silver_transform.py` e `dag_gold_refresh.py`):
```python
from helpers.lineage_emitter import lineage_callback

default_args = {
    ...
    "on_success_callback": lineage_callback,  # ← dispara automaticamente
}
```

**Como funciona internamente:**
```python
# Quando um task completa com sucesso:
def lineage_callback(context):
    ti = context["task_instance"]
    mapping = LINEAGE_MAP.get(ti.task_id)  # busca pelo task_id exato
    if not mapping:
        return
    emit_lineage(
        inputs=mapping["inputs"],     # ex: ["lakehouse.bronze.dados"]
        outputs=mapping["outputs"],   # ex: ["lakehouse.silver.gps"]
        job_name=f"{ti.dag_id}.{ti.task_id}",
        sql=mapping.get("sql"),       # query MERGE INTO completa
        column_lineage=mapping.get("column_lineage"),  # mapeamento coluna→coluna
    )
```

**O evento OpenLineage enviado:**
```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-05-04T12:00:00Z",
  "job": {"namespace": "dlh", "name": "dag_silver_transform.silver_gps"},
  "inputs": [{"namespace": "dlh", "name": "lakehouse.bronze.dados"}],
  "outputs": [{
    "namespace": "dlh",
    "name": "lakehouse.silver.gps",
    "facets": {
      "columnLineage": {
        "fields": {
          "latitude": {
            "inputFields": [{"namespace": "dlh", "name": "lakehouse.bronze.dados", "field": "payload"}]
          }
        }
      }
    }
  }],
  "producer": "https://github.com/infra-dissertacao/lineage_emitter"
}
```

**Endpoint de destino:** `POST http://openmetadata:8585/api/v1/openlineage/lineage`
**Autenticação:** Bearer token JWT (`OM_INGESTION_BOT_JWT`)

**Tasks mapeadas no LINEAGE_MAP (8 tasks):**

| task_id | inputs | outputs |
|---------|--------|---------|
| `ingerir_json_para_iceberg_bronze` | (nenhum) | `lakehouse.bronze.dados` |
| `silver_gps` | `lakehouse.bronze.dados` | `lakehouse.silver.gps` |
| `silver_sitrep` | `lakehouse.bronze.dados` | `lakehouse.silver.sitrep` |
| `silver_sensor` | `lakehouse.bronze.dados` | `lakehouse.silver.sensor` |
| `gold_posicionamento_atual` | `lakehouse.silver.gps` | `lakehouse.gold.posicionamento_atual` |
| `gold_sitrep_consolidado` | `lakehouse.silver.sitrep` | `lakehouse.gold.sitrep_consolidado` |
| `gold_latencia_por_batalhao` | `silver.gps + sitrep + sensor` | `lakehouse.gold.latencia_por_batalhao` |
| `gold_cobertura_temporal` | `lakehouse.silver.gps` | `lakehouse.gold.cobertura_temporal` |
| `gold_atividade_sensores` | `lakehouse.silver.sensor` | `lakehouse.gold.atividade_sensores` |

### 5.3 Mecanismo C: inject_lineage.py (bootstrap manual)

**Arquivo:** `infra/openmetadata/inject_lineage.py`

Script Python que injeta linhagem via REST API do OM (`PUT /api/v1/lineage`).
Diferente dos mecanismos A e B (que usam o endpoint OpenLineage), este usa a API nativa
do OM com IDs de tabelas.

**Quando usar:**
- Após reset do OM (`docker compose down -v`)
- Para injetar toda a linhagem de uma vez sem precisar rodar os DAGs
- Para adicionar novas relações de linhagem

**Como funciona:**
1. Login no OM como admin → obtém access token
2. Lista todas as tabelas do serviço `trino_lakehouse` → obtém UUIDs
3. Constrói edges de linhagem com `fromEntity` / `toEntity` (IDs das tabelas)
4. Inclui `sqlQuery` e `columnsLineage` em cada edge
5. Envia via `PUT /api/v1/lineage`

**Como executar:**
```bash
# De fora do Docker (localhost)
python3 infra/openmetadata/inject_lineage.py

# Ou especificando URL
python3 infra/openmetadata/inject_lineage.py --om-url http://localhost:8585
```

**Edges injetadas (10 no total):**
- `bronze.dados` → `silver.gps`
- `bronze.dados` → `silver.sitrep`
- `bronze.dados` → `silver.sensor`
- `silver.gps` → `gold.posicionamento_atual`
- `silver.sitrep` → `gold.sitrep_consolidado`
- `silver.gps` → `gold.latencia_por_batalhao`
- `silver.sitrep` → `gold.latencia_por_batalhao`
- `silver.sensor` → `gold.latencia_por_batalhao`
- `silver.gps` → `gold.cobertura_temporal`
- `silver.sensor` → `gold.atividade_sensores`

### Resultado esperado na UI do OM
Ao abrir qualquer tabela no OM → aba "Lineage":
- Grafo visual mostrando Bronze → Silver → Gold
- Ao clicar na aresta: mostra o SQL da transformação
- Ao expandir colunas: mostra qual coluna de origem alimenta qual coluna de destino

---

## 6. Feature 3: Sample Data (Amostras de Dados)

### O que é
Mostra exemplos de dados reais de cada tabela na UI do OM (aba "Sample Data").
Permite visualizar 50 linhas de amostra sem precisar executar queries manualmente.

### Como funciona

**Arquivo:** `airflow/dags/trino_sample_data_collector.py`

Esta DAG NÃO usa o SDK do OM. Usa REST API diretamente porque:
- O SDK de profiling tem problemas de JWT quando o OM é reiniciado
- A API REST com token de admin é mais confiável

**Fluxo:**
1. Login no OM como admin (`admin@open-metadata.org` / `admin`)
   - Endpoint: `POST /api/v1/users/login`
   - Password: base64 de "admin" → `YWRtaW4=`
   - Retorna: `accessToken` (válido por 24h)

2. Lista tabelas do serviço `trino_lakehouse`
   - Endpoint: `GET /api/v1/tables?limit=100&fields=columns`
   - Filtra por: `service.name == "trino_lakehouse"`

3. Para cada tabela, executa query no Trino via REST API
   - Endpoint: `POST http://trino:8090/v1/statement`
   - Query: `SELECT * FROM iceberg.bronze.dados LIMIT 50`
   - Header: `X-Trino-User: admin`
   - Usa polling com `nextUri` até `state == FINISHED`

4. Reordena colunas do resultado Trino para coincidir com a ordem do OM
   - O OM exige que as colunas do sample data estejam na mesma ordem do schema

5. Envia sample data para o OM
   - Endpoint: `PUT /api/v1/tables/{table_id}/sampleData`
   - Payload: `{"columns": ["col1", "col2"], "rows": [["val1", "val2"]]}`

6. Remove constraints NULL das colunas (cosmético)
   - O conector Trino marca todas as colunas com `constraint: "NULL"`
   - Isso polui a UI com "(NULL)" ao lado de cada coluna no lineage view
   - Endpoint: `PATCH /api/v1/tables/{table_id}` com JSON Patch

### Como executar
```bash
docker exec dlh_airflow_webserver airflow dags trigger trino_sample_data_collector
```

### Resultado esperado
Na UI do OM: abra qualquer tabela → aba "Sample Data" → deve mostrar ~50 linhas de dados.

---

## 7. Feature 4: Data Observability (Profiling/Estatísticas)

### O que é
Coleta estatísticas de cada coluna: contagem de nulls, min, max, média, desvio padrão,
histograma de valores. Aparece na UI na aba "Profiler & Data Quality".

### Como funciona

**Arquivo:** `airflow/dags/trino_lakehouse_profiler.py`

Esta DAG usa o SDK do OM (`metadata.workflow.metadata.MetadataWorkflow`) no modo Profiler:

```yaml
source:
  type: trino
  serviceName: trino_lakehouse
  serviceConnection:
    config:
      type: Trino
      hostPort: trino:8090
      username: trino
      catalog: iceberg
  sourceConfig:
    config:
      type: Profiler          # ← modo profiler (não metadata)
processor:
  type: orm-profiler           # ← executa queries de profiling
sink:
  type: metadata-rest
workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata:8585/api"
    authProvider: openmetadata  # ← auth automática via SDK
```

**O que o SDK faz internamente:**
1. Conecta ao Trino
2. Para cada tabela, executa queries como:
   - `SELECT COUNT(*), COUNT(DISTINCT col), MIN(col), MAX(col), AVG(col) FROM tabela`
   - `SELECT col, COUNT(*) FROM tabela GROUP BY col ORDER BY 2 DESC LIMIT 10`
3. Envia os resultados para o OM via REST

### Como executar
```bash
docker exec dlh_airflow_webserver airflow dags trigger trino_lakehouse_profiler
```

### Resultado esperado
Na UI do OM: abra qualquer tabela → aba "Profiler & Data Quality" → gráficos e
estatísticas de cada coluna.

---

## 8. Ordem de Execução Correta

**PASSO 0 — Subir infraestrutura e verificar:**
```bash
docker compose up -d
docker compose ps  # todos healthy
```

**PASSO 1 — Rodar o pipeline de dados (se ainda não rodou):**
```bash
# Via Airflow UI (localhost:8088) ou CLI:
docker exec dlh_airflow_webserver airflow dags trigger dag_ingestao_bronze
# Aguardar completar...
docker exec dlh_airflow_webserver airflow dags trigger dag_silver_transform
# Aguardar completar...
docker exec dlh_airflow_webserver airflow dags trigger dag_gold_refresh
```

**PASSO 2 — Criar o serviço Trino no OM (UI, uma vez):**
- `http://localhost:8585` → Settings → Database Services → Add → Trino
- Nome: `trino_lakehouse`, Host: `trino`, Port: `8090`, User: `trino`, Catalog: `iceberg`

**PASSO 3 — Ingestão de metadados (descobrir tabelas):**
```bash
docker exec dlh_airflow_webserver airflow dags trigger trino_lakehouse_metadata
```

**PASSO 4 — Injetar linhagem (escolha UM dos métodos):**
```bash
# Método A: Script standalone (recomendado para bootstrap)
python3 infra/openmetadata/inject_lineage.py

# Método B: Re-rodar os DAGs (a linhagem é emitida no on_success_callback)
docker exec dlh_airflow_webserver airflow dags trigger dag_silver_transform
docker exec dlh_airflow_webserver airflow dags trigger dag_gold_refresh
```

**PASSO 5 — Coletar sample data:**
```bash
docker exec dlh_airflow_webserver airflow dags trigger trino_sample_data_collector
```

**PASSO 6 — Executar profiler (data observability):**
```bash
docker exec dlh_airflow_webserver airflow dags trigger trino_lakehouse_profiler
```

---

## 9. Autenticação e JWT

### 9.1 Token do Ingestion-Bot

O OM usa um JWT para autenticar chamadas da API OpenLineage (lineage automático).

**Onde está definido:** Arquivo `.env`, variável `OM_INGESTION_BOT_JWT`

**Quem usa:**
- `infra/openlineage/openlineage.yml` → campo `auth.apiKey` (Spark listener)
- `airflow/dags/helpers/lineage_emitter.py` → variável de ambiente `OM_INGESTION_BOT_JWT`
- Docker Compose → passado como env var para Spark e Airflow

**Como gerar um novo token (se necessário):**
1. Acesse a UI do OM → Settings → Bots → `ingestion-bot`
2. Clique em "Revoke Token" → "Generate New Token"
3. Copie o novo JWT
4. Atualize em **3 lugares**:
   - `.env` → `OM_INGESTION_BOT_JWT=<novo_token>`
   - `infra/openlineage/openlineage.yml` → `auth.apiKey: <novo_token>`
   - Reinicie Spark e Airflow: `docker compose up -d`

**CUIDADO:** O JWT é gerado com as chaves RSA do OM. Se você fizer `docker compose down -v`
(apagando volumes), as chaves RSA são regeneradas e o token antigo fica INVÁLIDO.
Você precisará gerar um novo token pela UI.

### 9.2 Token Admin (para REST API)

Usado pelo `trino_sample_data_collector.py` e `inject_lineage.py`:
```python
# Login
resp = requests.post(f"{OM_API}/users/login",
    json={"email": "admin@open-metadata.org", "password": base64("admin")})
token = resp.json()["accessToken"]
# Válido por 24 horas
```

### 9.3 Autenticação automática do SDK

As DAGs `trino_lakehouse_metadata.py` e `trino_lakehouse_profiler.py` usam
`authProvider: openmetadata` na config do workflow. O SDK autentica automaticamente
com o servidor OM sem precisar de JWT manual.

---

## 10. Troubleshooting

### "Lineage não aparece na UI"

1. **Metadados foram ingeridos?** As tabelas precisam existir no OM primeiro.
   ```bash
   docker exec dlh_airflow_webserver airflow dags trigger trino_lakehouse_metadata
   ```

2. **Namespace correto?** O lineage_emitter usa `dlh` como namespace. Se o OM não
   resolver o namespace para o serviço `trino_lakehouse`, a linhagem é criada mas
   não vinculada às tabelas visíveis.

3. **JWT válido?** Verifique se `OM_INGESTION_BOT_JWT` está atualizado:
   ```bash
   curl -X POST http://localhost:8585/api/v1/openlineage/lineage \
     -H "Authorization: Bearer $(grep OM_INGESTION_BOT_JWT .env | cut -d= -f2)" \
     -H "Content-Type: application/json" \
     -d '{"eventType":"COMPLETE","eventTime":"2026-01-01T00:00:00Z","run":{"runId":"test"},"job":{"namespace":"dlh","name":"test"},"inputs":[],"outputs":[],"producer":"test","schemaURL":"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"}'
   ```
   Se retornar 401 → token inválido, regenere pela UI.

4. **Use o inject_lineage.py como alternativa:**
   ```bash
   python3 infra/openmetadata/inject_lineage.py
   ```

### "Sample data não aparece"

1. **Tabelas existem no OM?** Rode metadata ingestion primeiro.
2. **Trino tem dados?** Verifique: `docker exec dlh_trino trino --execute "SELECT count(*) FROM iceberg.bronze.dados"`
3. **Verifique logs:**
   ```bash
   docker exec dlh_airflow_webserver airflow tasks logs trino_sample_data_collector coletar_sample_data -1
   ```

### "Data Observability/Profiler não aparece"

1. **Metadados foram ingeridos?** O profiler precisa que as tabelas já existam no OM.
2. **SDK openmetadata-ingestion instalado?** Verifique no container do Airflow:
   ```bash
   docker exec dlh_airflow_webserver pip show openmetadata-ingestion
   ```
3. **Verifique logs do profiler:**
   ```bash
   docker exec dlh_airflow_webserver airflow tasks logs trino_lakehouse_profiler run_trino_profiler -1
   ```

### "Serviço trino_lakehouse não encontrado no OM"

Crie manualmente pela UI (veja Passo 2 na seção 8). Este serviço NÃO é criado
automaticamente — precisa ser registrado uma vez.

### "Cannot deserialize java.util.Date"

Bug conhecido com formato de data ISO. O lineage_emitter usa `.isoformat() + "Z"` que
é compatível. Se aparecer em outro contexto, use `.strftime('%Y-%m-%dT%H:%M:%S')`.

---

## 11. Mapa Completo de Arquivos

### Arquivos de infraestrutura (NÃO MODIFICAR)

| Arquivo | Função |
|---------|--------|
| `docker-compose.yml` (linhas 295-389) | Define os 4 containers do OM |
| `.env` | Credenciais, JWT, configurações |
| `infra/openlineage/openlineage.yml` | Config do Spark OpenLineage listener |
| `infra/spark/Dockerfile` (linha 17) | Download do JAR OpenLineage 1.18.0 |
| `infra/spark/spark-defaults.conf` (linha 28) | Ativa o listener `spark.extraListeners` |
| `infra/postgres/init/01_databases.sql` | Cria banco `openmetadata_db` e `airflow_om_db` |

### Arquivos de linhagem (NÃO MODIFICAR)

| Arquivo | Função |
|---------|--------|
| `airflow/dags/helpers/lineage_emitter.py` | Emitter OpenLineage com LINEAGE_MAP centralizado |
| `infra/openmetadata/inject_lineage.py` | Script bootstrap de linhagem via REST API |

### DAGs do pipeline (NÃO MODIFICAR — JÁ FUNCIONAM)

| Arquivo | Função |
|---------|--------|
| `airflow/dags/dag_ingestao_bronze.py` | Ingestão landing → Bronze |
| `airflow/dags/dag_silver_transform.py` | Bronze → Silver (com lineage callback) |
| `airflow/dags/dag_gold_refresh.py` | Silver → Gold (com lineage callback) |
| `airflow/dags/dag_iceberg_maintenance.py` | Manutenção Iceberg (compact, expire) |

### DAGs do OpenMetadata (podem ser ajustadas se necessário)

| Arquivo | Função |
|---------|--------|
| `airflow/dags/trino_lakehouse_metadata.py` | Ingestão de metadados (descoberta de tabelas) |
| `airflow/dags/trino_lakehouse_profiler.py` | Profiler (estatísticas de colunas) |
| `airflow/dags/trino_sample_data_collector.py` | Coleta de sample data via REST |

### Configurações de ingestão (referência)

| Arquivo | Função |
|---------|--------|
| `infra/openmetadata/ingestion/trino_lakehouse_metadata.yaml` | Config YAML do metadata workflow |
| `infra/openmetadata/ingestion/trino_profiler_custom.yaml` | Config YAML do profiler |

---

## Resumo Final

| Feature | Mecanismo | Arquivo Principal | Como Ativar |
|---------|-----------|-------------------|-------------|
| **Metadata** | SDK MetadataWorkflow | `trino_lakehouse_metadata.py` | Trigger manual da DAG |
| **Lineage** | REST OpenLineage + callback | `helpers/lineage_emitter.py` | Automático via `on_success_callback` dos DAGs |
| **Lineage** (bootstrap) | REST API nativa OM | `inject_lineage.py` | `python3 inject_lineage.py` |
| **Sample Data** | REST API direta (Trino + OM) | `trino_sample_data_collector.py` | Trigger manual da DAG |
| **Data Observability** | SDK MetadataWorkflow (Profiler) | `trino_lakehouse_profiler.py` | Trigger manual da DAG |

**Regra de ouro:** NÃO modifique as DAGs do pipeline. O lineage é emitido automaticamente
via callback. As 3 DAGs do OM (metadata, profiler, sample data) são complementares e
independentes do pipeline de dados.
