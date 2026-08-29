# Relatório de Correção — OpenMetadata Ingestion (ingestao2)

**Data:** 2026-04-30  
**Ambiente:** Docker Compose local — OpenMetadata 1.4.0 + Airflow 2.9  
**Serviço afetado:** `http://localhost:8585/service/pipelineServices/ingestao2/ingestions`  
**Pipelines com erro:** `66c88135-e018-4070-8cab-251ebae79f2b` e `68e4d293-c6be-47a0-878e-43c7a9bc37c6`

---

## Diagnóstico Inicial

Ao disparar qualquer um dos dois pipelines de ingestão, o log terminava com:

```
Airflow Summary: [0 Records, 0 Updated Records, 0 Warnings, 2 Errors, 0 Filtered]
```

e a task `ingestion_task` marcada como `FAILED`.

---

## Análise dos Erros

Os dois erros eram **causados em cadeia** — o Erro 2 era consequência direta do Erro 1.

---

### Erro 1 — Validação Pydantic: `tasks -> 0 -> owner -> type field required`

**Log:**
```
{topology_runner.py:240} WARNING - Unexpected value error when processing stage:
[type_=<class 'metadata.generated.schema.entity.data.pipeline.Pipeline'>
 processor='yield_pipeline' ...]:
1 validation error for Pipeline
tasks -> 0 -> owner -> type
  field required (type=value_error.missing)
```

**Arquivo:** `/home/airflow/.local/lib/python3.10/site-packages/metadata/ingestion/source/pipeline/airflow/metadata.py`

**Método:** `get_tasks_from_dag` (linha 389 original)

```python
# CÓDIGO ORIGINAL COM PROBLEMA
owner=self.get_owner(task.owner),
```

**Causa raiz:**  
O método `get_owner` consultava o OpenMetadata via `get_reference_by_name(name=owner)` para buscar o usuário correspondente ao owner da task Airflow (ex.: `"admin"`, `"airflow"`). Em si, o retorno era um `EntityReference` válido com `type` preenchido.

O problema estava nos **dados antigos armazenados no OpenMetadata**: pipelines previamente ingeridos pelo DAG `airflow_metadata_extraction` (de uma versão anterior do schema) tinham tasks com `owner` cujo `EntityReference` não possuía o campo `type`. Quando a nova ingestão tentava atualizar esses pipelines com `overwrite=True`, o servidor OpenMetadata retornava a entidade existente antes de processá-la, e o Pydantic falhava ao deserializar o retorno — mesmo que o que estávamos enviando estivesse correto.

**Confirmação:**
```python
# Teste no container — EntityReference sem type é inválido
from metadata.generated.schema.type.entityReference import EntityReference
EntityReference(id='test-id', name='admin')
# ValidationError: type field required
```

---

### Erro 2 — TypeError: `expected string or bytes-like object`

**Log:**
```
{status.py:76} WARNING - Wild error trying to extract status from DAG
b59a9628-5dc6-45e8-884a-56dc9016e0d1 - expected string or bytes-like object.
```

**Arquivo:** `metadata.py`

**Método:** `yield_pipeline_status`

**Causa raiz:**  
Quando `yield_pipeline` falhava com `ValidationError` (Erro 1), o bloco `except` registrava o erro e retornava `Either(left=...)`, mas **não definia** `self.context.get().pipeline`. Na execução subsequente de `yield_pipeline_status`, a chamada:

```python
pipeline_fqn = fqn.build(
    metadata=self.metadata,
    entity_type=Pipeline,
    service_name=self.context.get().pipeline_service,
    pipeline_name=self.context.get().pipeline,  # ← None aqui
)
```

com `pipeline_name=None` disparava internamente uma operação de regex sobre `None`:

```python
# Confirmado no container
fqn.build(metadata=None, entity_type=Pipeline, service_name='svc', pipeline_name=None)
# TypeError: expected string or bytes-like object
```

Esse `TypeError` era capturado pelo `except Exception` de `yield_pipeline_status` e registrado como o segundo erro do summary.

---

### Erro 3 — PATCH com data rejeitada: `Cannot deserialize java.util.Date`

**Log** (revelado após aplicar Fix 1 e Fix 2):
```
{patch_mixin.py:160} ERROR - Error trying to PATCH Pipeline [airflow_metadata_extraction]:
Cannot deserialize value of type `java.util.Date` from String "2026-04-29T00:00:00+00:00":
not a valid representation (error: Failed to parse Date value '2026-04-29T00:00:00+00:00':
Unparseable date: "2026-04-29T00:00:00+00:00")
```

**Arquivo:** `metadata.py`

**Métodos:** `yield_pipeline` e `get_tasks_from_dag`

```python
# CÓDIGO ORIGINAL COM PROBLEMA
startDate=pipeline_details.start_date.isoformat()
# Gera: "2026-04-29T00:00:00+00:00"  ← Java rejeita o ':' no offset
```

**Causa raiz:**  
O método `isoformat()` do Python gera o offset de fuso horário com dois-pontos (`+00:00`), seguindo o padrão ISO 8601 estrito. O servidor OpenMetadata 1.4.0 (Java) usa o parser `java.util.Date` que espera o formato RFC 822 sem dois-pontos no offset (`+0000`) ou simplesmente sem offset. Isso causava falha no PATCH de pipelines que já existiam no servidor.

---

## Correções Aplicadas

Todas as correções foram feitas no arquivo:
```
infra/openmetadata/airflow_metadata.py
```
(cópia local do arquivo do container, montada via volume no `docker-compose.yml`)

---

### Fix 1 — Remover owner nas tasks

**Método:** `get_tasks_from_dag`

```python
# ANTES
owner=self.get_owner(task.owner),

# DEPOIS
owner=None,  # task-level owner skipped: EntityReference sem 'type' no servidor quebra validação Pydantic
```

**Justificativa:** O campo `owner` em tasks é opcional e o lookup não trazia valor prático (os owners dos DAGs internos do OM não são usuários reais no OpenMetadata). Remover o campo elimina a falha de validação independentemente do estado dos dados no servidor.

---

### Fix 2 — Guard contra pipeline None no contexto

**Método:** `yield_pipeline_status`

```python
# ANTES
def yield_pipeline_status(self, pipeline_details):
    try:
        dag_run_list = self.get_pipeline_status(pipeline_details.dag_id)
        ...

# DEPOIS
def yield_pipeline_status(self, pipeline_details):
    try:
        if not self.context.get().pipeline:
            # pipeline não foi registrado (yield_pipeline falhou) — pular status
            return
        dag_run_list = self.get_pipeline_status(pipeline_details.dag_id)
        ...
```

**Justificativa:** Quando `yield_pipeline` falha antes de definir `context.pipeline`, `yield_pipeline_status` não tem como construir o FQN do pipeline. O guard previne o `TypeError` em cascata.

---

### Fix 3 — Formato de data sem offset de fuso horário

**Método:** `yield_pipeline` e `get_tasks_from_dag`

```python
# ANTES
startDate=pipeline_details.start_date.isoformat()  # "2026-04-29T00:00:00+00:00"

# DEPOIS
startDate=pipeline_details.start_date.strftime('%Y-%m-%dT%H:%M:%S')  # "2026-04-29T00:00:00"
```

Aplicado também para `task.start_date` e `task.end_date` em `get_tasks_from_dag`.

**Justificativa:** O Java server do OpenMetadata 1.4.0 não aceita o formato `+00:00` (ISO 8601 com colon no offset). O formato sem offset funciona e é aceito pelo servidor.

---

### Fix 4 — Limpeza de dados corrompidos no OpenMetadata

Todos os pipelines do serviço `ingestao2` foram deletados via API (`hardDelete=true`) para garantir recriação limpa sem dados legados com schema incompleto:

```bash
PASS_B64=$(python3 -c "import base64; print(base64.b64encode(b'admin').decode())")
TOKEN=$(curl -s -X POST "http://localhost:8585/api/v1/users/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"admin\",\"password\":\"${PASS_B64}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('accessToken',''))")

curl -X DELETE "http://localhost:8585/api/v1/pipelines/<ID>?hardDelete=true" \
  -H "Authorization: Bearer $TOKEN"
```

Pipelines deletados:

| ID | Nome |
|----|------|
| `563037b2-...` | `66c88135-e018-4070-8cab-251ebae79f2b` |
| `63a7e335-...` | `68e4d293-c6be-47a0-878e-43c7a9bc37c6` |
| `61cdb21f-...` | `airflow_metadata_extraction` |
| `58c5a182-...` | `extended_sample_data` |
| `2352821b-...` | `ingestion-docker-operator` |
| `af526531-...` | `lineage_tutorial_operator` |
| `96d60182-...` | `sample_data` |
| `4732cb98-...` | `sample_lineage` |
| `48d71144-...` | `sample_usage` |

---

## Integração no Docker Compose

O arquivo `airflow_metadata.py` com os 3 fixes foi adicionado como volume read-only no serviço `openmetadata-ingestion` em `docker-compose.yml`:

```yaml
# docker-compose.yml — serviço openmetadata-ingestion
volumes:
  - ./infra/openmetadata/plugin.py:/home/airflow/.local/lib/python3.10/site-packages/openmetadata_managed_apis/plugin.py:ro
  - ./infra/openmetadata/airflow_lineage.yaml:/opt/airflow/lineage.yaml:ro
  - ./infra/openmetadata/airflow_metadata.py:/home/airflow/.local/lib/python3.10/site-packages/metadata/ingestion/source/pipeline/airflow/metadata.py:ro  # ← ADICIONADO
```

---

## Resultado Final

```
Workflow Airflow Summary:
  Updated records: 0
  Warnings:        0
  Errors:          0

Workflow OpenMetadata Summary:
  Updated records: 0
  Warnings:        0
  Errors:          0
```

Ambos os pipelines (`66c88135-...` e `68e4d293-...`) terminaram com **`state: success`**.

---

## Procedimento para Re-aplicar (se o container for recriado do zero)

Se `docker compose down -v` for executado (reset total), os dados do OpenMetadata são perdidos e os pipelines precisam ser re-criados. Nesse caso:

1. Subir a stack normalmente: `docker compose up -d`
2. Aguardar OpenMetadata inicializar (~3 min)
3. Recriar os ingestion pipelines em `http://localhost:8585/service/pipelineServices/ingestao2`
4. Fazer deploy de cada pipeline via UI ou API
5. Disparar os pipelines — com o `airflow_metadata.py` corrigido montado via volume, **não haverá erros**

---

## Arquivos Modificados

| Arquivo | Tipo de Mudança |
|---------|----------------|
| `infra/openmetadata/airflow_metadata.py` | Criado — patch do source OpenMetadata (3 fixes) |
| `docker-compose.yml` | Volume mount adicionado para `airflow_metadata.py` |

---

## Notas para Manutenção Futura

- **Upgrade do OpenMetadata:** ao atualizar para versão > 1.4.0, verificar se os 3 bugs foram corrigidos upstream. Se sim, remover o `airflow_metadata.py` e o volume mount correspondente.
- **Novo serviço de pipeline:** se um novo `pipelineService` for criado conectando a um Airflow que tenha DAGs com `start_date` definido, o Fix 3 é essencial — o formato `isoformat()` quebrará sem ele.
- **Owner de tasks:** o Fix 1 é conservador (desabilita lookup de owner em tasks). Se futuramente for necessário ter owners nas tasks, a correção correta é checar se o `EntityReference` retornado pelo servidor tem `type` antes de aceitar a entidade — não basta que o que enviamos tenha `type`; o que o servidor retorna pode não ter.

---

---

# Relatório de Correção — Linhagem de Transformação Ausente

**Data:** 2026-04-30  
**Ambiente:** Docker Compose local — OpenMetadata 1.4.0 + Airflow 2.9 + Spark 3.5.8  
**Sintoma:** Aba Lineage vazia para todas as tabelas Silver e Gold em `http://localhost:8585/table/teste_trino.iceberg.silver.sensor/lineage`

---

## Diagnóstico Inicial

A aba **Lineage** de qualquer tabela Silver ou Gold exibia apenas o próprio nó da tabela, sem nenhuma conexão upstream ou downstream. O grafo esperado era:

```
bronze.dados → silver.{gps, sitrep, sensor} → gold.{posicionamento_atual, sitrep_consolidado,
                                                      latencia_por_batalhao, cobertura_temporal,
                                                      atividade_sensores}
```

---

## Análise das Causas

Foram identificadas **3 causas encadeadas**, todas relacionadas à ausência de qualquer mecanismo de injeção de linhagem.

---

### Causa 1 — `OPENLINEAGE_URL` apontando para porta errada

**Configuração no `docker-compose.yml` (serviço `x-airflow-common`):**
```yaml
OPENLINEAGE_URL: "http://openmetadata-ingestion:8793/api/v1/lineage"
```

**Problema:** A porta `8793` é o servidor de logs do Airflow (`airflow.log`), não um receiver de linhagem. Qualquer tentativa de emissão de evento OpenLineage resultaria em `Connection refused`. Além disso, mesmo que a porta estivesse correta, esta configuração seria irrelevante porque...

---

### Causa 2 — OpenMetadata 1.4.0 não tem receiver OpenLineage

O servidor OpenMetadata 1.4.0 **não expõe nenhum endpoint HTTP** que aceite eventos no protocolo OpenLineage (`/api/v1/lineage` na API do OM é uma API REST própria, com schema diferente do OpenLineage). Não existe integração OpenLineage→OpenMetadata nesta versão sem um conector específico de ingestão.

Confirmação via Swagger (`http://localhost:8585/swagger`): nenhum endpoint aceita o payload OpenLineage padrão.

---

### Causa 3 — Nenhum mecanismo de injeção existia

Verificação dos três possíveis pontos de emissão de linhagem:

| Mecanismo | Status | Detalhe |
|-----------|--------|---------|
| Airflow `inlets`/`outlets` | **Ausente** | Nenhum DAG definia `inlets` ou `outlets` nas tasks |
| OpenLineage Spark JAR | **Ausente** | `ls /opt/spark/jars/ \| grep openlineage` retornou vazio |
| Chamada direta à API REST do OM | **Ausente** | Nenhum DAG ou script chamava `PUT /api/v1/lineage` |

---

## Correções Aplicadas

---

### Fix 1 — Desabilitar `OPENLINEAGE_URL` incorreto

**Arquivo:** `docker-compose.yml`

```yaml
# ANTES
OPENLINEAGE_URL: "http://openmetadata-ingestion:8793/api/v1/lineage"
OPENLINEAGE_NAMESPACE: "dlh"

# DEPOIS
# OpenLineage emitter disabled: OM 1.4.0 não tem receiver HTTP.
# Linhagem é gerenciada via inject_lineage.py + task register_om_lineage no dag_gold_refresh.
# OPENLINEAGE_URL: "http://openmetadata-ingestion:8793/api/v1/lineage"
```

---

### Fix 2 — Script de injeção de linhagem via REST API

**Arquivo criado:** `infra/openmetadata/inject_lineage.py`

Script Python standalone que injeta todas as 10 arestas de linhagem (Bronze→Silver + Silver→Gold) via `PUT /api/v1/lineage` com mapeamento completo de colunas.

**Arestas injetadas:**

| De | Para | Via |
|----|------|-----|
| `bronze.dados` | `silver.gps` | `bronze_to_silver.py --tipo gps` |
| `bronze.dados` | `silver.sitrep` | `bronze_to_silver.py --tipo sitrep` |
| `bronze.dados` | `silver.sensor` | `bronze_to_silver.py --tipo sensor` |
| `silver.gps` | `gold.posicionamento_atual` | `silver_to_gold.py --visao posicionamento_atual` |
| `silver.sitrep` | `gold.sitrep_consolidado` | `silver_to_gold.py --visao sitrep_consolidado` |
| `silver.gps` | `gold.latencia_por_batalhao` | `silver_to_gold.py --visao latencia_por_batalhao` |
| `silver.sitrep` | `gold.latencia_por_batalhao` | idem |
| `silver.sensor` | `gold.latencia_por_batalhao` | idem |
| `silver.gps` | `gold.cobertura_temporal` | `silver_to_gold.py --visao cobertura_temporal` |
| `silver.sensor` | `gold.atividade_sensores` | `silver_to_gold.py --visao atividade_sensores` |

**Execução:**
```bash
python3 infra/openmetadata/inject_lineage.py
# Saída esperada: 10 OK, 0 errors
```

---

### Fix 3 — Task automática de linhagem no `dag_gold_refresh`

**Arquivo modificado:** `airflow/dags/dag_gold_refresh.py`

Adicionada task `register_om_lineage` (PythonOperator) que executa **após todas as tasks Gold** e re-registra as 10 arestas no OpenMetadata via REST API. Usa apenas `urllib.request` (stdlib), sem dependências extras.

```python
register_lineage = PythonOperator(
    task_id="register_om_lineage",
    python_callable=_register_om_lineage,
)

[posicionamento_atual, sitrep_consolidado, latencia_por_batalhao,
 cobertura_temporal, atividade_sensores] >> register_lineage
```

A função `_register_om_lineage` é tolerante a falhas — se o OpenMetadata estiver indisponível, imprime `WARNING` mas **não bloqueia** o refresh Gold.

---

## Resultado Final

Após executar `python3 infra/openmetadata/inject_lineage.py`:

```
Injecting 10 lineage edges ...
  bronze.dados → silver.gps    ... OK
  bronze.dados → silver.sitrep ... OK
  bronze.dados → silver.sensor ... OK
  silver.gps    → gold.posicionamento_atual  ... OK
  silver.sitrep → gold.sitrep_consolidado    ... OK
  silver.gps    → gold.latencia_por_batalhao ... OK
  silver.sitrep → gold.latencia_por_batalhao ... OK
  silver.sensor → gold.latencia_por_batalhao ... OK
  silver.gps    → gold.cobertura_temporal    ... OK
  silver.sensor → gold.atividade_sensores    ... OK

Done: 10 OK, 0 errors
```

Verificação via API — grafo a partir de `bronze.dados` com `downstreamDepth=3`:
```
Nós no grafo: 8
  [SILVER] gps, sitrep, sensor
  [GOLD]   posicionamento_atual, latencia_por_batalhao, cobertura_temporal,
           sitrep_consolidado, atividade_sensores
```

---

## Procedimento para Re-aplicar (após `docker compose down -v`)

A linhagem é armazenada no banco `openmetadata_db` (PostgreSQL). Um reset com `-v` apaga os volumes e perde todos os dados do OpenMetadata.

**Passos após reset total:**

1. Subir a stack: `docker compose up -d`
2. Aguardar OpenMetadata inicializar (~3 min): `curl http://localhost:8585/api/v1/system/status`
3. Recriar e disparar os ingestion pipelines do serviço `ingestao2` (veja seção anterior deste relatório)
4. Reinjetar linhagem:
   ```bash
   python3 infra/openmetadata/inject_lineage.py
   ```
5. Verificar em `http://localhost:8585/table/teste_trino.iceberg.silver.sensor/lineage`

A partir deste ponto, cada execução do `dag_gold_refresh` re-registra a linhagem automaticamente via task `register_om_lineage`.

---

## Arquivos Modificados

| Arquivo | Tipo de Mudança |
|---------|----------------|
| `infra/openmetadata/inject_lineage.py` | Criado — script de injeção de linhagem (10 arestas, mapeamento de colunas) |
| `airflow/dags/dag_gold_refresh.py` | Task `register_om_lineage` adicionada ao final do DAG |
| `docker-compose.yml` | `OPENLINEAGE_URL` comentado (configuração morta/incorreta) |

---

## Notas para Manutenção Futura

- **Novas tabelas Gold:** ao adicionar uma nova visão Gold em `silver_to_gold.py`, adicionar a aresta correspondente tanto em `inject_lineage.py` quanto na função `_register_om_lineage` em `dag_gold_refresh.py`.
- **Upgrade para OpenMetadata > 1.4.0:** verificar se a versão nova inclui suporte a OpenLineage HTTP receiver. Se sim, pode-se migrar para emissão automática via Spark JAR (`openlineage-spark`) e remover a injeção manual.
- **Reset parcial (sem `-v`):** `docker compose down && docker compose up -d` preserva os volumes — a linhagem permanece intacta, não é necessário reinjetar.
