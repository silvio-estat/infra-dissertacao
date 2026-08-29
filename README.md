# infra-dissertacao

Infraestrutura da parte empírica da dissertação de mestrado **ARQ_TEMAC** —
comparação entre o paradigma **Lakehouse** (Apache Iceberg + Trino) e o paradigma
**Relacional** (PostgreSQL) para um Sistema de Comando e Controle (C2) do Exército
Brasileiro, avaliada por 7 indicadores GQM / ISO 25010.

Ambiente 100% local, open-source, orquestrado por **Docker Compose** — sem Kubernetes
e sem serviços de nuvem.

---

## Stack

| Papel | Tecnologia |
|---|---|
| Object Storage | MinIO (S3-compatible) |
| Table Format | Apache Iceberg 1.5.0 |
| Catálogo | Hive Metastore (backend PostgreSQL) |
| Processamento | Apache Spark 3.5.8 (Standalone) |
| Query Engine | Trino 437 |
| Orquestração | Apache Airflow 2.9 |
| Governança | OpenMetadata 1.12.5 (+ Elasticsearch) |
| Baseline relacional | PostgreSQL 15 |

---

## Arquitetura Medallion

```
scripts/gerar_dados.py → MinIO landing/
  → Bronze (lakehouse.bronze.dados)              append-only, todos os tipos juntos
  → Silver (lakehouse.silver.{gps,sitrep,sensor}) MERGE INTO, deduplicado, latência/fora_de_ordem
  → Gold   (lakehouse.gold.*)                     5 visões analíticas do COC
  → PostgreSQL baseline_db                        espelho do Silver (comparação)
```

**Cenário simulado:** 7 batalhões (1BPE–5BPE, 1BIB–2BIB) gerando dados de GPS, SITREP
e Sensor com atrasos realistas (conectividade degradada), na área de Brasília
(lat −15.77, lon −47.92).

**Nomes de catálogo:** `lakehouse.*` no Spark/PySpark — `iceberg.*` no Trino.

---

## Como subir

Pré-requisitos: Docker + Docker Compose.

```bash
cp .env.example .env          # credenciais de ambiente local
docker compose up -d          # respeita a ordem de dependências
docker compose ps             # conferir serviços UP
```

> Nunca use `docker compose restart` — não reaplica variáveis de ambiente.
> Use sempre `docker compose up -d`.

### Portas

| Serviço | URL |
|---|---|
| MinIO Console | http://localhost:9001 |
| Spark Master | http://localhost:8081 |
| Trino | http://localhost:8090 |
| Airflow | http://localhost:8080 |
| OpenMetadata | http://localhost:8585 |

Credenciais de desenvolvimento local estão em `.env.example`.

---

## Pipeline (DAGs Airflow)

| DAG | Função |
|---|---|
| `dag_ingestao_bronze` | Ingestão landing → Bronze |
| `dag_silver_transform` | Bronze → Silver (MERGE INTO, deduplicação) |
| `dag_gold_refresh` | Silver → Gold (visões do COC) |
| `dag_iceberg_maintenance` | Compaction, expire snapshots, remove orphans |
| `dag_baseline_sync` | Silver → PostgreSQL `baseline_db` (via Trino) |
| `dag_benchmark_escalabilidade` | Benchmark parametrizado por volume (trigger manual) |
| `dag_trino_governance` | Testes de qualidade / observabilidade via OpenMetadata |

Coleta dos indicadores: `scripts/coletar_metricas_gqm.py`.

---

## Estrutura do repositório

```
docker-compose.yml       stack completa
.env.example             modelo de variáveis de ambiente (.env nunca é commitado)
airflow/dags/            DAGs do pipeline + helpers/ (lineage emitter)
spark/jobs/              jobs PySpark: bronze_ingestor, bronze_to_silver, silver_to_gold, iceberg_maintenance
scripts/                 gerar_dados.py (gerador sintético), coletar_metricas_gqm.py
infra/
  ├── spark/             Dockerfile + spark-defaults.conf
  ├── trino/catalog/     iceberg.properties + postgresql.properties
  ├── postgres/init/     01_databases.sql (bancos + schema baseline + views analíticas)
  ├── minio/             configuração/inicialização de buckets
  ├── openmetadata/      configuração de governança e linhagem
  └── openlineage/       openlineage.yml (linhagem automática Spark → OpenMetadata)
relatorios/              relatórios de arquitetura, benchmark e guias de reprodução
setup_venv.sh            ambiente Python local (rodar o gerador fora do Docker)
```

---

## Notas de operação

- **Bronze é imutável** — apenas append; nunca DELETE/UPDATE na camada Bronze.
- **Trino não expande `${VAR}`** em arquivos `.properties` — as credenciais S3 ficam
  hardcoded em `infra/trino/catalog/iceberg.properties`.
- **Credenciais S3A no Spark** devem ser passadas explicitamente via `--conf` no
  `spark-submit` (o `spark-defaults.conf` usa `${VAR}`, que pode não expandir dentro
  do container).
- O `gerador de dados` roda fora do Docker e acessa o MinIO em `localhost:9000`.

Detalhes de troubleshooting e histórico de setup estão em `relatorios/`.
