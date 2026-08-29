# Guia — Data Observability no OpenMetadata para tabelas Trino (Iceberg)

**Data:** 2026-05-02
**Versões:** OpenMetadata 1.12.5 · Trino 437 · Airflow 2.9 · Iceberg 1.5.0
**Pré-requisito:** tabelas já catalogadas no OM (ingestão de metadados concluída)

---

## 1. O que é o Data Observability no OM

A aba **Data Observability** (anteriormente "Profiler") exibe estatísticas reais das tabelas:

- **Table profile:** rowCount, columnCount, timestamp da última coleta
- **Column profile:** nullCount, nullProportion, uniqueCount, distinctCount, min, max, mean, etc.
- **Histórico temporal:** gráficos de evolução das métricas ao longo do tempo

Essas métricas são coletadas pelo **Profiler Workflow** do SDK `openmetadata-ingestion`, que conecta diretamente ao banco de dados (Trino), executa queries de profiling, e envia os resultados para o OM server.

---

## 2. Por que não funcionava

O Profiler Workflow precisa de autenticação via **Bot JWT** (não aceita token de admin). O problema era uma cadeia de 3 falhas encadeadas:

### 2.1. O OM server gera novas chaves RSA a cada boot

O OM armazena chaves RSA em `/opt/openmetadata/conf/private_key.der` e `public_key.der`. Essas chaves são **geradas na build da imagem** e ficam dentro do container — sem volume mount, elas são estáveis desde que o container não seja recriado. Porém, durante atualizações de versão ou `docker compose up -d --force-recreate`, o container é recriado com novas chaves, invalidando todos os JWTs existentes.

### 2.2. O ingestion-bot não tinha JWT armazenado

O campo `authenticationMechanism.config.JWTToken` da entidade `ingestion-bot` no banco de dados estava **vazio**. Sem um token de referência, o OM rejeitava qualquer JWT com:

```
Not Authorized! The given token does not match the current bot's token!
```

### 2.3. A API REST não permite setar o token diretamente

- `PUT /users/{id}/generateToken` → 404 (endpoint não existe no 1.12.5)
- `PATCH /users/{id}` com `authenticationMechanism` → aceita (200) mas não persiste o JWT no formato correto quando consultado via GET
- O token é armazenado **criptografado com Fernet** no banco — a API não expõe o token descriptografado

---

## 3. Solução implementada

A resolução envolveu 3 passos:

### Passo 1 — Gerar JWT com a chave RSA do OM server

```python
import jwt
from cryptography.hazmat.primitives.serialization import (
    load_der_private_key, Encoding, PrivateFormat, NoEncryption
)

# Copiar a chave do container OM para gerar o JWT
# docker cp dlh_openmetadata:/opt/openmetadata/conf/private_key.der /tmp/

with open("/tmp/private_key.der", "rb") as f:
    private_key = load_der_private_key(f.read(), password=None)

private_key_pem = private_key.private_bytes(
    encoding=Encoding.PEM,
    format=PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=NoEncryption()
).decode("utf-8")

payload = {
    "sub": "ingestion-bot",
    "email": "ingestion-bot@open-metadata.org",
    "isBot": True,
    "iss": "open-metadata.org",
    "iat": int(time.time()),
    "tokenType": "BOT",
    "username": "ingestion-bot",
    "preferred_username": "ingestion-bot",
    "roles": ["IngestionBotRole"]
}

new_jwt = jwt.encode(payload, private_key_pem, algorithm="RS256",
    headers={"kid": "Gb389a-9f76-gdjs-a92j-0242bk94356"})
```

**Nota sobre o `kid`:** O valor `Gb389a-9f76-gdjs-a92j-0242bk94356` é o default do OM. Se for personalizado, buscar em `openmetadata.yaml` → `jwtTokenConfiguration.keyId`.

### Passo 2 — Criptografar com Fernet e gravar no banco

O OM armazena o JWT criptografado com Fernet no campo `authenticationMechanism.config.JWTToken` da entidade `user_entity`. A chave Fernet usada é configurada em `openmetadata.yaml` → `fernetConfiguration.fernetKey`.

```python
from cryptography.fernet import Fernet

# Chave Fernet do OM (default ou a configurada no openmetadata.yaml)
OM_FERNET_KEY = "jJ/9sz0g0OHxsfxOoSfdFdmk3ysNmPRnH3TUAbz3IHA="
f = Fernet(OM_FERNET_KEY.encode())
encrypted_jwt = "fernet:" + f.encrypt(new_jwt.encode()).decode()
```

```sql
-- Atualizar o JWT no banco do OM
UPDATE user_entity
SET json = jsonb_set(
    json::jsonb,
    '{authenticationMechanism,config,JWTToken}',
    '"<encrypted_jwt_aqui>"'::jsonb
)
WHERE json::jsonb->>'name' = 'ingestion-bot';
```

### Passo 3 — Reiniciar o OM server para limpar cache

```bash
docker compose up -d --force-recreate openmetadata
```

O OM cacheia o token do bot em memória. Após a atualização no banco, é necessário reiniciar para que o servidor releia o token.

**Importante:** Usar `--force-recreate` neste caso NÃO regenera as chaves RSA porque elas são geradas na build da imagem, não no entrypoint. As chaves só mudam se a imagem for reconstruída.

### Passo 4 — Salvar no .env

Atualizar a variável `OM_INGESTION_BOT_JWT` no `.env` com o JWT gerado (não criptografado):

```
OM_INGESTION_BOT_JWT=eyJhbGciOiJSUzI1NiIs...
```

---

## 4. Executar o Profiler

Com o JWT válido, o Profiler é executado no container `dlh_openmetadata_ingestion`:

```bash
docker exec dlh_openmetadata_ingestion python3 -c "
import yaml, os
from metadata.workflow.profiler import ProfilerWorkflow

config = yaml.safe_load('''
source:
  type: trino
  serviceName: trino_lakehouse
  sourceConfig:
    config:
      type: Profiler
      computeMetrics: true
      profileSample: 100
processor:
  type: orm-profiler
  config: {}
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  openMetadataServerConfig:
    hostPort: \"http://openmetadata:8585/api\"
    authProvider: openmetadata
    securityConfig:
      jwtToken: \"$OM_INGESTION_BOT_JWT\"
''')

wf = ProfilerWorkflow.create(config)
wf.execute()
wf.print_status()
wf.stop()
"
```

### Resultado esperado

```
Workflow Profiler Summary:
  Processed records: 312   (métricas de todas as colunas)
  Errors: 0
  Success %: 100.0

Workflow finished in time: 55s
```

---

## 5. Onde rodar o Profiler

| Opção | Container | Funciona |
|---|---|---|
| OM Ingestion container | `dlh_openmetadata_ingestion` | **SIM** — tem SDK + Trino connector + JWT |
| Nosso Airflow scheduler | `dlh_airflow_scheduler` | **NÃO** — SDK `openmetadata-ingestion` não instalado |
| Via OM UI | Settings → Services → Trino → Run Profiler | **SIM** — se JWT estiver válido |

O Profiler **deve** rodar no container `dlh_openmetadata_ingestion` porque ele tem o SDK `openmetadata-ingestion==1.12.5.6` com os conectores corretos.

---

## 6. Parâmetros do Profiler

```yaml
source:
  type: trino
  serviceName: trino_lakehouse    # nome do serviço no OM
  sourceConfig:
    config:
      type: Profiler
      computeMetrics: true        # calcular métricas (nullCount, uniqueCount, etc.)
      profileSample: 100          # % de linhas a amostrar (100 = tabela inteira)
      # tableFilterPattern:       # filtrar tabelas específicas
      #   includes:
      #     - bronze.dados
      #     - silver.*
processor:
  type: orm-profiler
  config: {}
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "<OM_INGESTION_BOT_JWT>"
```

---

## 7. Troubleshooting

| Sintoma | Causa | Solução |
|---|---|---|
| "The given token does not match the current bot's token" | JWT no `.env` não bate com o armazenado no banco | Regenerar JWT (Seção 3) |
| "VersionMismatchException: Server 1.12.5 vs Client 1.4.0" | SDK desatualizado no container errado | Rodar no `dlh_openmetadata_ingestion`, não no nosso Airflow |
| "No implementation found for trino" (WARNING) | Normal — o system profiler do Trino não é implementado | Ignorar — não afeta as métricas de coluna |
| Profile não aparece no `GET /tables?fields=profile` | API de listagem não inclui campos pesados | Verificar via UI ou `GET /tables/{id}?fields=profile` |
| Profile não aparece na UI após rodar | Cache do OM server | Esperar 1-2 minutos ou fazer F5 na página |

---

## 8. Diferença entre Sample Data e Data Observability

| Aspecto | Sample Data | Data Observability (Profiler) |
|---|---|---|
| O que mostra | Linhas de exemplo da tabela | Estatísticas (counts, proporções, min/max) |
| Como coleta | Query `SELECT * LIMIT N` | Queries de agregação (`COUNT`, `COUNT DISTINCT`, etc.) |
| Onde roda | Nosso Airflow (REST API) | Container `dlh_openmetadata_ingestion` (SDK) |
| Autenticação | Token admin (login) | Bot JWT (RSA + Fernet) |
| DAG | `trino_sample_data_collector` | Profiler workflow (SDK) |
| Persistência | `PUT /tables/{id}/sampleData` | Via SDK → `profiler_data_time_series` no banco |

---

## 9. Checklist para ambiente de produção

1. **Persistir chaves RSA:** Montar volume para `/opt/openmetadata/conf/` ou gerar chaves externamente e injetar via volume bind
2. **Fernet key:** Configurar `FERNET_KEY` explicitamente no OM server (não usar o default)
3. **JWT sem expiração:** O payload usa `"tokenType": "BOT"` sem `exp` — válido indefinidamente
4. **Rotação de JWT:** Necessária apenas quando as chaves RSA mudam (rebuild da imagem OM)
5. **Agendamento:** Configurar profiler via OM UI (Settings → Services → Add Ingestion → Profiler) com schedule diário/semanal
6. **ProfileSample:** Em produção com tabelas grandes, usar `profileSample: 10` ou `profileSample: 1` para não sobrecarregar o Trino

---

## 10. Como verificar se o JWT ainda é válido

```bash
# Teste rápido de validade
docker exec dlh_openmetadata_ingestion bash -c '
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $OM_INGESTION_BOT_JWT" \
  http://openmetadata:8585/api/v1/services/databaseServices/name/trino_lakehouse?fields=connection
'
# 200 = válido, 401 = precisa regenerar
```

---

## 11. Chaves e parâmetros de referência

| Parâmetro | Localização | Default |
|---|---|---|
| RSA private key | `dlh_openmetadata:/opt/openmetadata/conf/private_key.der` | Gerada na build |
| RSA public key | `dlh_openmetadata:/opt/openmetadata/conf/public_key.der` | Gerada na build |
| Fernet key (OM) | `openmetadata.yaml` → `fernetConfiguration.fernetKey` | `jJ/9sz0g0OHxsfxOoSfdFdmk3ysNmPRnH3TUAbz3IHA=` |
| JWT kid | `openmetadata.yaml` → `jwtTokenConfiguration.keyId` | `Gb389a-9f76-gdjs-a92j-0242bk94356` |
| JWT issuer | `openmetadata.yaml` → `jwtTokenConfiguration.jwtissuer` | `open-metadata.org` |
| Bot user name | Fixo | `ingestion-bot` |
| Bot email | Fixo | `ingestion-bot@open-metadata.org` |

---

*Gerado em 2026-05-02 — baseado em implementação funcional testada no ambiente infra-dissertacao*
