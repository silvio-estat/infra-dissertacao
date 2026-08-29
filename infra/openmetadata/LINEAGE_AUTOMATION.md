# Manual — Automação de Linhagem via OpenLineage

**Contexto:** Este manual descreve o que precisa ser feito para substituir a injeção manual de linhagem (`inject_lineage.py` + task `register_om_lineage`) por captura automática via protocolo OpenLineage direto dos jobs Spark.

**Pré-requisito mínimo:** OpenMetadata ≥ 1.5.0 (a versão 1.4.0 em uso não tem receiver OpenLineage nativo).

---

## O que muda com a automação

Hoje, a linhagem é injetada manualmente — qualquer nova tabela exige editar dois arquivos Python. Com OpenLineage, o Spark emite eventos automaticamente a cada `df.writeTo(...)` ou `MERGE INTO`, e o OpenMetadata os absorve sem intervenção.

---

## Passo 1 — Atualizar o OpenMetadata para ≥ 1.5.0

No `docker-compose.yml`, trocar a versão dos dois serviços:

```yaml
# ANTES
openmetadata-ingestion:
  image: openmetadata/ingestion:1.4.0
openmetadata:
  image: openmetadata/server:1.4.0

# DEPOIS
openmetadata-ingestion:
  image: openmetadata/ingestion:1.5.0
openmetadata:
  image: openmetadata/server:1.5.0
```

> **Atenção:** fazer backup do volume `postgres_data` antes do upgrade. O OM executa migrações de schema no banco automaticamente, mas não há rollback.

---

## Passo 2 — Adicionar o JAR do OpenLineage ao Spark

Baixar o JAR compatível com Spark 3.5.x e adicioná-lo à imagem:

**`infra/spark/Dockerfile`** — adicionar ao final do bloco de downloads:

```dockerfile
# OpenLineage listener para captura automática de linhagem
ARG OPENLINEAGE_VERSION=1.18.0
RUN curl -fsSL \
  "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OPENLINEAGE_VERSION}/openlineage-spark_2.12-${OPENLINEAGE_VERSION}.jar" \
  -o /opt/spark/jars/openlineage-spark.jar
```

Versão recomendada: `1.18.0` (compatível com Spark 3.5.x e testada com OM 1.5.x).  
Verificar última versão em: `https://search.maven.org/artifact/io.openlineage/openlineage-spark_2.12`

---

## Passo 3 — Configurar o listener no Spark

**`infra/spark/spark-defaults.conf`** — adicionar:

```properties
spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://openmetadata:8585
spark.openlineage.transport.endpoint=/api/v1/lineage/openlineage
spark.openlineage.namespace=dlh
spark.openlineage.appName=infra-dissertacao
```

> O endpoint `/api/v1/lineage/openlineage` é o receiver OpenLineage nativo do OM ≥ 1.5.0.

---

## Passo 4 — Remover a injeção manual

Com o listener ativo, a injeção manual se torna redundante. Remover:

1. **`airflow/dags/dag_gold_refresh.py`** — remover a função `_register_om_lineage`, o import de `PythonOperator` e a task `register_lineage`. Restaurar a linha de dependências para:
   ```python
   [posicionamento_atual, sitrep_consolidado, latencia_por_batalhao, cobertura_temporal, atividade_sensores]
   ```

2. **`docker-compose.yml`** — reativar e corrigir o `OPENLINEAGE_URL` no serviço `x-airflow-common` (para emissão de linhagem também pelos operadores Airflow, opcional):
   ```yaml
   OPENLINEAGE_URL: "http://openmetadata:8585"
   OPENLINEAGE_NAMESPACE: "dlh"
   ```

3. **`infra/openmetadata/inject_lineage.py`** — pode ser deletado ou mantido como utilitário de bootstrap para resets.

---

## Passo 5 — Testar

1. Subir a stack: `docker compose up -d --build`
2. Rodar um job Spark qualquer: `dag_silver_transform` ou `dag_gold_refresh`
3. Verificar em `http://localhost:8585/table/teste_trino.iceberg.silver.sensor/lineage` — a linhagem deve aparecer automaticamente após o job terminar
4. Adicionar uma nova tabela Silver ao `bronze_to_silver.py` e rodar novamente — confirmar que a linhagem da nova tabela aparece **sem nenhuma alteração manual**

---

## Resumo do que muda em cada arquivo

| Arquivo | Ação |
|---------|------|
| `docker-compose.yml` | Versão OM 1.4.0 → 1.5.0; reativar `OPENLINEAGE_URL` |
| `infra/spark/Dockerfile` | Adicionar download do `openlineage-spark.jar` |
| `infra/spark/spark-defaults.conf` | Adicionar 5 linhas de configuração do listener |
| `airflow/dags/dag_gold_refresh.py` | Remover função `_register_om_lineage` e task `register_lineage` |
| `infra/openmetadata/inject_lineage.py` | Deletar ou manter só para bootstrap pós-reset |
| `infra/openmetadata/airflow_metadata.py` | Verificar se os 3 fixes ainda são necessários na versão nova do OM |
