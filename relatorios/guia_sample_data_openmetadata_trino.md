# Guia — Sample Data no OpenMetadata para tabelas Trino (Iceberg)

**Data:** 2026-05-01
**Versões testadas:** OpenMetadata 1.12.5 · Trino 437 · Airflow 2.9 · Iceberg 1.5.0
**Arquivo da DAG:** `airflow/dags/trino_sample_data_collector.py`

---

## 1. Contexto e por que o caminho oficial não funciona

O OpenMetadata oferece um **Profiler** nativo que deveria coletar sample data automaticamente. Ele funciona via SDK Python (`openmetadata-ingestion`) rodando dentro do Airflow. Na prática, essa abordagem falha neste stack por dois motivos:

1. **JWT do ingestion-bot invalida a cada reinício do OM server** — o OM gera novas chaves RSA em cada boot, mas o JWT armazenado no `.env` continua apontando para as chaves antigas. O SDK exige autenticação via bot (não aceita token de admin).

2. **O SDK tem acoplamento forte com a versão do server** — `openmetadata-ingestion==1.4.0` rejeita server 1.12.5 com `VersionMismatchException`, e a versão 1.12.5 do SDK muda o formato de autenticação (`/v1/` em vez de `/api/v1/`).

A solução que funciona é uma **DAG custom que usa as APIs REST do Trino e do OM diretamente**, sem dependência do SDK `openmetadata-ingestion`.

---

## 2. Pré-requisitos

| Componente | Requisito |
|---|---|
| Rede Docker | Airflow, Trino e OM devem estar na **mesma rede** Docker |
| Trino | Endpoint REST acessível (porta 8090 neste projeto) |
| OpenMetadata | Tabelas já catalogadas via ingestão de metadados |
| Airflow | Pacote `requests` disponível (incluso na imagem padrão) |
| Credenciais OM | Login admin funcional (`admin@open-metadata.org` / `admin`) |

### Verificar conectividade (rodar de dentro do container Airflow)

```bash
# Airflow → Trino
curl -s -o /dev/null -w '%{http_code}' http://trino:8090/v1/info
# Esperado: 200

# Airflow → OpenMetadata
curl -s -o /dev/null -w '%{http_code}' http://openmetadata:8585/api/v1/system/version
# Esperado: 200
```

---

## 3. Como funciona a DAG

O fluxo tem 4 etapas, todas em uma única task Python:

```
Login OM (admin) → Listar tabelas → Query Trino → PUT sample data no OM
```

### 3.1. Autenticação no OpenMetadata

```python
import requests, base64

OM_API = "http://openmetadata:8585/api/v1"
senha_b64 = base64.b64encode(b"admin").decode()

resp = requests.post(
    f"{OM_API}/users/login",
    json={"email": "admin@open-metadata.org", "password": senha_b64},
)
token = resp.json()["accessToken"]
```

**Ponto crítico:** Usa-se o token de **admin** (não o bot JWT). O token de admin tem validade de 24h e funciona para todas as operações REST. O SDK do OM não aceita token de admin, mas a API REST sim.

**Em produção:** trocar a senha hardcoded por uma variável de ambiente ou Airflow Variable.

### 3.2. Listar tabelas do serviço

```python
headers = {"Authorization": f"Bearer {token}"}
resp = requests.get(
    f"{OM_API}/tables",
    headers=headers,
    params={"limit": 100, "fields": "columns"},
)
tabelas = [
    t for t in resp.json()["data"]
    if t.get("service", {}).get("name") == "trino_lakehouse"
]
```

O parâmetro `fields=columns` é obrigatório — sem ele, a resposta não inclui os nomes das colunas, que são necessários para montar o payload de sample data.

### 3.3. Query no Trino via REST API

```python
TRINO_URL = "http://trino:8090"

resp = requests.post(
    f"{TRINO_URL}/v1/statement",
    data="SELECT * FROM iceberg.bronze.dados LIMIT 50",  # PLAIN TEXT, não JSON
    headers={"X-Trino-User": "admin"},
)
```

**Armadilhas documentadas:**

| Armadilha | Errado | Certo |
|---|---|---|
| Formato do body | `json={"query": sql}` | `data=sql` (plain text) |
| `nextUri` | `requests.get(TRINO_HOST + next_uri)` | `requests.get(next_uri)` — já é URL absoluta |
| Resultados paginados | Retornar só a primeira página | Acumular rows de **todas** as páginas |
| Estado FINISHED sem data | Assumir que data vem junto com FINISHED | Data pode vir em páginas anteriores ao FINISHED |

#### Fluxo de paginação do Trino REST API

```
POST /v1/statement → resposta com nextUri
  ↓
GET nextUri → pode ter data parcial + novo nextUri
  ↓
GET nextUri → mais data + novo nextUri
  ↓
GET nextUri → state=FINISHED, sem nextUri → fim
```

O campo `data` pode aparecer em **qualquer** resposta intermediária — é preciso acumular com `linhas.extend(resultado["data"])`. A query termina quando `nextUri` é `null`.

#### Conversão de FQN

O OM armazena o FQN como `trino_lakehouse.iceberg.schema.tabela`. Para consultar no Trino, remover o primeiro segmento (nome do serviço OM):

```
trino_lakehouse.iceberg.bronze.dados → iceberg.bronze.dados
```

### 3.4. Enviar sample data para o OpenMetadata

```python
payload = {
    "columns": ["col1", "col2", "col3"],   # lista de strings
    "rows": [                                # lista de listas de strings
        ["valor1", "valor2", "valor3"],
        ["valor4", "valor5", "valor6"],
    ]
}

requests.put(
    f"{OM_API}/tables/{tabela_id}/sampleData",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json=payload,
)
```

**Regras do endpoint `PUT /tables/{id}/sampleData`:**

1. **Método:** `PUT` (não PATCH, não POST)
2. **`columns`:** lista de **strings** com os nomes das colunas — devem coincidir exatamente com os nomes registrados no OM
3. **`rows`:** lista de listas — cada valor deve ser **string** (converter com `str(v)`)
4. **Valores nulos:** converter para string vazia `""`
5. **Ordem das colunas:** deve corresponder à ordem registrada no OM, não à ordem retornada pelo Trino

**Verificação:** O `PUT` retorna 200 e inclui `sampleData` na resposta. Porém, o endpoint `GET /tables?fields=sampleData` (listagem em massa) **não** retorna o campo. Para verificar, usar o endpoint dedicado:

```
GET /api/v1/tables/{id}/sampleData
```

---

## 4. Bug visual do "NULL" na view de lineage

O conector Trino do OM seta `constraint: "NULL"` (a string literal, não JSON null) em todas as colunas. Isso significa "a coluna aceita valores nulos", mas a UI do OM exibe o texto "NULL" ao lado de cada coluna na view de lineage, o que confunde.

**Correção via API:**

```python
# Para cada tabela, remover o campo constraint das colunas
patches = [
    {"op": "remove", "path": f"/columns/{i}/constraint"}
    for i, col in enumerate(tabela["columns"])
    if col.get("constraint") == "NULL"
]

requests.patch(
    f"{OM_API}/tables/{tabela_id}",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json-patch+json",  # JSON Patch, não merge
    },
    json=patches,
)
```

A DAG `trino_sample_data_collector` já faz essa limpeza automaticamente a cada execução.

---

## 5. Adaptação para produção

### 5.1. Credenciais

Substituir o login hardcoded por Airflow Variables ou Secrets Backend:

```python
from airflow.models import Variable

OM_ADMIN_EMAIL = Variable.get("om_admin_email")
OM_ADMIN_PASSWORD = Variable.get("om_admin_password")
```

### 5.2. Agendamento

A DAG atual é `schedule_interval=None` (disparo manual). Para produção, agendar conforme a frequência de ingestão de dados:

```python
schedule_interval="0 6 * * *"  # diário às 06h
```

### 5.3. Múltiplos serviços

Para cobrir mais de um serviço Trino, parametrizar o nome do serviço:

```python
SERVICOS = ["trino_lakehouse", "trino_analytics"]

tabelas = [
    t for t in resp.json()["data"]
    if t.get("service", {}).get("name") in SERVICOS
]
```

### 5.4. Conexão Trino via Airflow Connection

Em vez de URL hardcoded, usar `airflow.hooks.base.BaseHook`:

```python
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("trino_default")
TRINO_URL = f"http://{conn.host}:{conn.port}"
```

### 5.5. Volume de dados

O `SAMPLE_LIMIT` está em 50 linhas. Para tabelas muito grandes, isso é suficiente para visualização. Se o OM estiver lento, reduzir para 10–20.

### 5.6. Bot JWT (alternativa ao login admin)

Se em produção o JWT do bot for estável (chaves RSA persistidas via volume mount), substituir o login admin por:

```python
token = os.environ["OM_INGESTION_BOT_JWT"]
# Usar diretamente no header Authorization
```

Isso elimina a dependência de senha admin e é mais seguro.

---

## 6. Endpoints da API utilizados

| Operação | Método | Endpoint |
|---|---|---|
| Login OM | `POST` | `/api/v1/users/login` |
| Listar tabelas | `GET` | `/api/v1/tables?limit=100&fields=columns` |
| Obter tabela por FQN | `GET` | `/api/v1/tables/name/{fqn}` |
| Enviar sample data | `PUT` | `/api/v1/tables/{id}/sampleData` |
| Verificar sample data | `GET` | `/api/v1/tables/{id}/sampleData` |
| Atualizar colunas (patch) | `PATCH` | `/api/v1/tables/{id}` |
| Executar query Trino | `POST` | `/v1/statement` |
| Buscar resultado Trino | `GET` | URL retornada em `nextUri` |

---

## 7. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| Login OM retorna 401 | Senha ou email errado | Email é `admin@open-metadata.org` (com hífen), senha em base64 |
| PUT sampleData retorna 400 "Invalid column name" | Nome da coluna não existe no OM | Usar exatamente os nomes retornados em `GET /tables?fields=columns` |
| PUT sampleData retorna 400 "Cannot deserialize" | Coluna enviada como objeto, não string | `columns` deve ser lista de strings, não lista de objetos |
| Query Trino retorna 200 mas sem data | Resultado paginado | Seguir `nextUri` até que seja null, acumulando `data` |
| Query Trino falha com "Table not found" | FQN mal convertido | Remover apenas o primeiro segmento: `serviço.catalogo.schema.tabela` → `catalogo.schema.tabela` |
| "NULL" aparece na lineage | `constraint: "NULL"` nas colunas | Executar PATCH para remover o campo constraint |
| Sample data não aparece no `GET /tables?fields=sampleData` | Comportamento normal do OM 1.12.5 | Verificar via `GET /tables/{id}/sampleData` (endpoint dedicado) |

---

*Gerado em 2026-05-01 — baseado em implementação funcional testada no ambiente infra-dissertacao*
