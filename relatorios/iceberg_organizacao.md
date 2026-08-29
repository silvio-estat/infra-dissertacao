# Como o Apache Iceberg se Organiza neste Projeto

**Projeto:** infra-dissertacao — Lakehouse Medallion PoC  
**Contexto:** ARQ_TEMAC — Sistema C2 Exército Brasileiro  
**Data:** 2026-04-29

---

## 1. A Confusão Comum

Ao abrir o MinIO pela primeira vez, a expectativa natural é encontrar isto:

```
bucket: bronze/      ← um bucket por zona Medallion
bucket: silver/
bucket: gold/
```

O que existe neste projeto é diferente:

```
bucket: landing/     ← JSONs brutos dos batalhões
bucket: lakehouse/   ← warehouse Iceberg (todas as zonas)
  warehouse/
    bronze.db/       ← isso confunde
      dados/
    silver.db/
      gps/
      sitrep/
      sensor/
    gold.db/
      operational_picture/
```

Ambas as organizações são válidas. Este projeto usa **um único bucket com
subdiretórios** em vez de um bucket por zona. A razão disso está nos três
conceitos que o Iceberg empilha em cima do S3/MinIO.

---

## 2. Os Três Níveis do Iceberg

O Iceberg tem uma hierarquia de três camadas, análoga a um banco de dados
relacional:

```
CATÁLOGO  →  DATABASE (namespace)  →  TABELA
lakehouse →  bronze                →  dados
lakehouse →  silver                →  gps
lakehouse →  silver                →  sitrep
lakehouse →  silver                →  sensor
```

Em SQL, os três níveis sempre aparecem juntos:

```sql
SELECT * FROM lakehouse.bronze.dados
SELECT * FROM lakehouse.silver.gps
```

### 2.1 Nível 1 — Catálogo (`lakehouse`)

O catálogo é o ponto de entrada: sabe quais tabelas existem, onde cada uma
fica fisicamente armazenada e qual é o schema atual de cada uma.

Neste projeto o catálogo é o **Hive Metastore**, configurado em
`infra/spark/spark-defaults.conf`:

```
spark.sql.catalog.lakehouse           = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type      = hive
spark.sql.catalog.lakehouse.uri       = thrift://hive-metastore:9083
spark.sql.catalog.lakehouse.warehouse = s3a://lakehouse/warehouse
```

O Hive Metastore guarda seus registros num banco PostgreSQL (container
`dlh_postgres`). É lá que fica a lista de tabelas, schemas e localizações
físicas — não no MinIO.

> **Ponto-chave:** o catálogo vive no PostgreSQL, não no MinIO. O MinIO guarda
> apenas os dados e metadados Iceberg das tabelas. São coisas separadas.

### 2.2 Nível 2 — Database / Namespace (`bronze`, `silver`, `gold`)

Database no Iceberg é um agrupador lógico de tabelas. No storage físico ele
vira um **diretório** dentro do warehouse. Por convenção Hive, esse diretório
recebe o sufixo `.db`:

```
s3a://lakehouse/warehouse/bronze.db/    ← diretório, não arquivo
s3a://lakehouse/warehouse/silver.db/
s3a://lakehouse/warehouse/gold.db/
```

> **O `bronze.db` não é um arquivo SQLite.** Não é um banco de dados em si —
> é apenas uma pasta. O sufixo `.db` é herança da convenção Hive e não
> significa nada de especial no S3/MinIO.

O código que cria esse namespace em `spark/jobs/bronze_ingestor.py`:

```python
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.bronze")
```

### 2.3 Nível 3 — Tabela (`dados`, `gps`, `sitrep`, `sensor`)

Cada tabela é um diretório com duas subpastas gerenciadas pelo Iceberg:

```
s3a://lakehouse/warehouse/bronze.db/dados/
├── metadata/
│   ├── v1.metadata.json        ← schema e configurações (versão inicial)
│   ├── v2.metadata.json        ← nova versão criada a cada write
│   ├── snap-XXXXXXX.avro       ← lista de arquivos do snapshot
│   └── manifest-XXXXXXX.avro  ← índice e estatísticas de cada .parquet
└── data/
    ├── timestamp_chegada_day=2025-04-01/
    │   └── batalhao_origem=1BIM/
    │       └── 00000-XXXXXXX.parquet
    └── timestamp_chegada_day=2025-04-02/
        └── batalhao_origem=1BIM/
            └── 00001-XXXXXXX.parquet
```

A estrutura de partições (`timestamp_chegada_day=...`, `batalhao_origem=...`)
vem diretamente da definição da tabela em `bronze_ingestor.py`:

```python
PARTITIONED BY (days(timestamp_chegada), batalhao_origem)
```

---

## 3. O que Cada Arquivo Faz

### `metadata/vN.metadata.json` — A certidão de nascimento da tabela

Contém: schema atual, histórico de schemas (evolução de colunas), lista de
snapshots, configurações de compressão e particionamento. Toda vez que se
escreve na tabela, o Iceberg cria um novo arquivo `vN+1.metadata.json` e
atualiza o ponteiro no catálogo. O arquivo anterior **não é deletado** — isso
é o que permite time travel.

### `metadata/snap-*.avro` — O snapshot

Cada write (INSERT, MERGE, etc.) cria um snapshot. Um snapshot é um ponteiro
para um conjunto de manifests. É o que permite consultas do tipo:

```sql
SELECT * FROM lakehouse.bronze.dados FOR VERSION AS OF 42
SELECT * FROM lakehouse.bronze.dados FOR TIMESTAMP AS OF '2025-04-01'
```

### `metadata/manifest-*.avro` — O índice dos dados

Lista todos os arquivos `.parquet` que fazem parte de um snapshot, com
estatísticas por coluna (mínimo, máximo, contagem de nulos). Isso é o que
permite ao Iceberg **pular arquivos** que não contêm dados relevantes para
uma query — equivalente ao *predicate pushdown* em bancos colunares.

Por exemplo: uma query `WHERE batalhao_origem = '3BIM'` lê o manifest,
identifica quais `.parquet` contêm dados de `3BIM` e ignora todos os outros,
sem nem abri-los.

### `data/*.parquet` — Os dados reais

Arquivos Parquet comprimidos com Snappy (configurado em `bronze_ingestor.py`
via `TBLPROPERTIES`). Organizados em partições-diretório, por isso o caminho
`timestamp_chegada_day=.../batalhao_origem=.../`.

---

## 4. Por que Um Único Bucket e Não um Bucket por Zona?

Neste projeto há **dois buckets no MinIO**:

| Bucket     | Propósito                                              |
|------------|--------------------------------------------------------|
| `landing`  | Zona de pouso dos JSONs brutos, lidos pelo Spark       |
| `lakehouse`| Warehouse Iceberg: Bronze + Silver + Gold              |

A separação `landing` vs `lakehouse` é intencional: o `landing` é descartável
(dados brutos que já foram ingeridos podem ser removidos); o `lakehouse` é o
dado sob controle do Iceberg, com ACID, time travel e metadados estruturados.

Dentro do `lakehouse`, Bronze/Silver/Gold são **namespaces** (pastas), não
buckets separados. Essa escolha tem trade-offs:

| Critério                | Um bucket por zona         | Um bucket, namespaces (este projeto) |
|-------------------------|----------------------------|--------------------------------------|
| Controle de acesso S3   | Mais granular por bucket   | Requer políticas por prefixo         |
| Overhead operacional    | Mais buckets para gerenciar| Um ponto único de gerenciamento      |
| Custo de PoC            | Desnecessariamente complexo| Adequado                             |
| Escalabilidade futura   | Facilita isolamento        | Requer refatoração se necessário     |

Para uma PoC em Docker Compose, namespaces dentro de um bucket é a escolha
correta. Em produção com times distintos por zona, buckets separados são
recomendados para isolamento de permissões.

---

## 5. Como o Spark Resolve uma Tabela

Quando o código em `bronze_ingestor.py` escreve:

```python
df_bronze.writeTo("lakehouse.bronze.dados").append()
```

O Spark executa a seguinte cadeia de resolução:

```
1. "lakehouse"  → consulta spark.sql.catalog.lakehouse
                → Hive Metastore (PostgreSQL na porta 9083)

2. "bronze"     → namespace registrado no Metastore
                → localização física: s3a://lakehouse/warehouse/bronze.db/

3. "dados"      → tabela registrada no Metastore
                → localização: s3a://lakehouse/warehouse/bronze.db/dados/

4. Iceberg lê   → metadata/vN.metadata.json
                → descobre snapshots, manifests e arquivos de dados

5. Spark acessa → data/*.parquet via S3A → MinIO
```

O Hive Metastore é o intermediário obrigatório. Sem ele, o Spark não saberia
que `lakehouse.bronze.dados` existe. É por isso que o container
`hive-metastore` sobe antes do Spark e do Trino no `docker-compose.yml`.

---

## 6. Como o Trino Acessa as Mesmas Tabelas

O Trino aponta para o **mesmo Hive Metastore**. Quando se executa no Trino:

```sql
SELECT * FROM lakehouse.bronze.dados LIMIT 10;
```

O Trino faz o mesmo caminho: Metastore → localização S3 → lê os Parquet
diretamente do MinIO, **sem passar pelo Spark**. Isso é o que torna o Iceberg
*engine-agnostic*: os dados ficam no MinIO com metadados abertos, e qualquer
engine que entenda o protocolo Iceberg (Spark, Trino, Flink, DuckDB) consegue
ler sem intermediário.

---

## 7. Diagrama Completo da Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                      VISÃO LÓGICA (SQL)                      │
│                                                              │
│  CATÁLOGO: lakehouse                                         │
│  ├── DATABASE: bronze                                        │
│  │   └── TABLE: dados          (todos os tipos, raw)         │
│  ├── DATABASE: silver                                        │
│  │   ├── TABLE: gps            (coordenadas validadas)       │
│  │   ├── TABLE: sitrep         (situações de combate)        │
│  │   └── TABLE: sensor         (telemetria de drones)        │
│  └── DATABASE: gold                                          │
│      └── TABLE: operational_picture  (visão integrada C2)   │
└──────────────────────────┬───────────────────────────────────┘
                           │  resolve via
                           ▼
┌──────────────────────────────────────────────────────────────┐
│           HIVE METASTORE  (backend: PostgreSQL)              │
│                                                              │
│  Guarda: nomes de tabelas, localização física no S3,         │
│  schemas, informações de partição                            │
│                                                              │
│  Acessado por: Spark (porta 9083) e Trino (porta 9083)       │
└──────────────────────────┬───────────────────────────────────┘
                           │  aponta para
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                   MINIO  (object storage)                    │
│                                                              │
│  bucket: landing/                                            │
│    └── [JSONs brutos dos batalhões — zona de pouso]          │
│                                                              │
│  bucket: lakehouse/                                          │
│    warehouse/                                                │
│    ├── bronze.db/          ← apenas um diretório             │
│    │   └── dados/                                            │
│    │       ├── metadata/   ← schema, snapshots, índices      │
│    │       └── data/       ← arquivos .parquet particionados │
│    ├── silver.db/                                            │
│    │   ├── gps/                                              │
│    │   │   ├── metadata/                                     │
│    │   │   └── data/                                         │
│    │   ├── sitrep/  (idem)                                   │
│    │   └── sensor/  (idem)                                   │
│    └── gold.db/                                              │
│        └── operational_picture/  (idem)                      │
└──────────────────────────────────────────────────────────────┘
```

---

## 8. Resumo — Um Conceito por Linha

| Conceito            | O que é                                                        |
|---------------------|----------------------------------------------------------------|
| Bucket MinIO        | Armário físico. Criado uma vez, aponta o warehouse para ele.   |
| `bronze.db`         | Gaveta dentro do armário. Apenas um diretório. `.db` é Hive.  |
| `warehouse/`        | Prefixo raiz de todas as tabelas Iceberg dentro do bucket.     |
| Tabela Iceberg      | Pasta com `metadata/` e `data/`. O Iceberg gerencia ambas.     |
| `metadata/vN.json`  | Schema e histórico de versões. Base do time travel.            |
| `metadata/snap.avro`| Fotografia dos dados em um instante (ACID snapshot).           |
| `metadata/mfst.avro`| Índice de arquivos com estatísticas (viabiliza pular leitura). |
| `data/*.parquet`    | Onde os dados moram de fato, comprimidos com Snappy.           |
| Hive Metastore      | Catálogo de endereços: nome → localização física no MinIO.     |
| PostgreSQL          | Onde o Metastore persiste seus registros.                      |
| Spark / Trino       | Engines de leitura/escrita. Ambas leem o mesmo MinIO.          |
