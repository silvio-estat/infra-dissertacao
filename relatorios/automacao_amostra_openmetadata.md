# Automação da coleta de amostra de dados no OpenMetadata

## Objetivo
Garantir que, sempre que uma nova tabela for criada/registrada no catálogo Iceberg (via Trino) e reconhecida pelo OpenMetadata, uma amostra de dados seja coletada automaticamente e armazenada na aba **Sample Data** da UI do OpenMetadata.

## Componentes envolvidos
| Componente | Função |
|------------|--------|
| **OpenMetadata** | Emite eventos de criação de entidade (TableCreated). |
| **Airflow** (container `dlh_airflow_webserver`) | Executa a DAG `trino_profiler` que roda o profiler do OpenMetadata. |
| **Plugin Airflow (`infra/openmetadata/plugin.py`)** | Exponibiliza um endpoint HTTP `/om/table_created` que recebe o webhook e dispara a DAG. |
| **DAG `trino_profiler`** (`airflow/dags/airflow_trino_profiler.py`) | Invoca o `MetadataWorkflow` do OpenMetadata; aceita opcionalmente o parâmetro `table_fqn` para focar o profiling em uma única tabela. |
| **Trino + Iceberg** | Fonte de dados a ser profilada. |

## Passos de implementação

### 1. Plugin Airflow – webhook
- Editado `infra/openmetadata/plugin.py`.
- Foi criado um `Blueprint` chamado `template_blueprint` que agora inclui a rota:
```python
@template_blueprint.route("/om/table_created", methods=["POST"])
def table_created_webhook():
    payload = request.get_json(force=True)
    table_fqn = payload.get("entityFQN") or payload.get("tableFQN")
    if not table_fqn:
        return jsonify({"error": "missing table_fqn in payload"}), 400
    client = Client(None, None)
    client.trigger_dag(dag_id="trino_profiler", conf={"table_fqn": table_fqn})
    return jsonify({"status": "triggered", "dag_id": "trino_profiler", "table_fqn": table_fqn}), 200
```
- Importa `request`, `jsonify` (Flask) e `Client` (Airflow local client).
- O endpoint aceita o JSON enviado pelo OpenMetadata e dispara a DAG `trino_profiler` passando o **FQN** da tabela como `conf`.

### 2. DAG de profiling
- Arquivo `airflow/dags/airflow_trino_profiler.py` foi adaptado.
- A função `metadata_ingestion_workflow(**context)` agora:
  1. Recupera `table_fqn` de `dag_run.conf` (ou de `params`).
  2. Monta dinamicamente o YAML de configuração do profiler. Quando `table_fqn` está presente, adiciona:
  ```yaml
  entityFullyQualifiedName: "<table_fqn>"
  ```
  Isso limita o profiling à tabela recém‑criada.
  3. Executa o workflow como antes.
- O `PythonOperator` permanece inalterado; o próprio Airflow passa o **context** para a callable.

### 3. Reinício do Airflow
```bash
docker compose restart dlh_airflow_webserver   # ou up -d para recriar
```
O novo Blueprint é registrado e o endpoint fica disponível em `http://<host‑airflow>:8080/om/table_created`.

### 4. Configurar o webhook no OpenMetadata
1. Acesse a UI do OpenMetadata (`http://localhost:8585`, admin/admin).  
2. Navegue até **Settings → Event Subscriptions** → **Add subscription**.
3. Preencha:
   - **Name:** `Trigger Trino Profiler on Table Creation`
   - **Event Type:** `EntityCreated` (filtre por `entityType = table`).
   - **Destination:** `Webhook`
   - **URL:** `http://dlh_airflow_webserver:8080/om/table_created`
   - **Headers:** `Content-Type: application/json` (opcional).
   - **Payload:** deixe o padrão (OpenMetadata enviará o objeto da entidade). 
4. Salve.

### 5. Fluxo de teste
```bash
# 1) Crie uma tabela via Spark/Trino ou UI do OpenMetadata
# 2) OpenMetadata gera o evento TableCreated → webhook → Airflow
# 3) Airflow dispara a DAG trino_profiler (única tabela ou todas)
# 4) Ao terminar, abra http://localhost:8585 e verifique a aba "Sample Data"
#    da tabela recém‑criada.
```

### 6. (Opcional) Segurança do webhook
- Caso queira restringir o acesso, adicione um cabeçalho `X-Auth-Token` ao webhook e valide esse token dentro da rota antes de disparar a DAG.
- O código do endpoint pode ser estendido com:
```python
if request.headers.get('X-Auth-Token') != os.getenv('WEBHOOK_TOKEN'):
    return jsonify({"error": "unauthorized"}), 403
```
- Defina a variável de ambiente `WEBHOOK_TOKEN` no container do Airflow.

## Checklist de verificação
- [ ] Plugin Airflow carregado (`docker logs dlh_airflow_webserver | grep /om/table_created`).
- [ ] Subscription criada no OpenMetadata.
- [ ] Criação de tabela dispara o endpoint (verifique logs do Airflow). 
- [ ] DAG `trino_profiler` executa e finaliza sem erros.
- [ ] Amostra de dados visível na UI do OpenMetadata.

---
*Documento mantido em* `relatorios/automacao_amostra_openmetadata.md` *para consulta futura.*
