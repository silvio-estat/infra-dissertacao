# infra-dissertacao

Infraestrutura da parte empírica da dissertação de mestrado **ARQ_TEMAC**: um **estudo de caso**
de arquitetura **Lakehouse** (MinIO + Apache Iceberg + Trino + Spark) para um Sistema de Comando e
Controle (C2) do Exército Brasileiro.

O trabalho sustenta duas teses:

1. **Heterogeneidade**: ingerir e integrar dados de tipos variados (JSON, texto livre, imagem,
   planilha, PDF escaneado, áudio) sob um modelo canônico único.
2. **Linhagem**: rastrear qualquer valor até o arquivo bruto que o originou, incluindo qual
   modelo de IA fez a extração e com que versão de prompt.

Ambiente 100% local e open-source, orquestrado por **Docker Compose**, sem Kubernetes e sem nuvem.
Os dados são **sintéticos** (Operação Perseu 2024): nenhuma unidade, lugar ou fato é real.

---

## Stack

| Papel | Tecnologia |
|---|---|
| Armazenamento de objetos | MinIO (compatível com S3) |
| Formato de tabela | Apache Iceberg 1.5.0 |
| Catálogo das tabelas | Hive Metastore (sobre PostgreSQL) |
| Processamento | Apache Spark 3.5.8 |
| Consulta SQL | Trino 437 |
| Orquestração | Apache Airflow 2.9 |
| Governança e linhagem | OpenMetadata 1.12.5 (+ Elasticsearch) |
| Leitura de planilha | openpyxl |
| OCR de PDF escaneado | tesseract 5 (português) |
| Tabela de PDF escaneado | Docling (serviço `docling-serve`, RapidOCR) |
| Transcrição de voz | faster-whisper `medium` |
| Modelo de linguagem | Ollama + `qwen3.5:4b`, **na máquina, fora do Docker** |
| Infraestrutura (metastore, Airflow, OpenMetadata) | PostgreSQL 15 |

---

## Arquitetura

```
scripts/geradores/   →  MinIO  s3://lakehouse/landing/perseu_2024/<fonte>/
                          │
   DAG 1_ingestao         ▼
                     BRONZE  RECEPCAO_BRUTA + ARQUIVO        o que chegou, sem mexer (só acrescenta)
                          │
   DAG 2_bronze_extracao  ▼
                     BRONZE  EXTRACAO + AJUSTE_EXTRACAO      o conteúdo tirado de dentro dos binários,
                          │                                  com a ferramenta, o modelo e o prompt usados
   DAG 3_silver_evento    ▼
                     SILVER  EVENTO + SITUACAO_UNIDADE       o de/para do modelo canônico
                             + MEDIDA_COORDENACAO
                             REF_OPERACAO, REF_UNIDADE, REF_GAZETTEER, ...
                          │
   DAG 4_gold_visoes      ▼
                     GOLD    PITCIC + PROBLEMA_DADOS         uma tabela por visão, pelas fases do processo
                             + PPCOT + COC + AMC

   DAG 5_governanca  →  OpenMetadata: catálogo, perfil, amostra, qualidade e linhagem
```

As regras de transformação ficam em `canonico/modelo_canonico.yaml`, e não no código. O job
`spark/jobs/canonico_para_silver.py` lê o YAML e monta o SQL. Integrar uma fonte nova é
acrescentar um bloco no YAML e uma tarefa na DAG 3.

**Nomes de catálogo:** `lakehouse.*` no Spark e `iceberg.*` no Trino. São as mesmas tabelas.

### As seis fontes

| Fonte | O que chega | Como o conteúdo é lido |
|---|---|---|
| C2_A | JSON (posições, medidas de coordenação) | direto |
| C2_B | JSON com relato em texto livre + fotos | modelo de linguagem lê o relato |
| RELPER | planilha `.xlsx` e cópias escaneadas em PDF | openpyxl / Docling |
| FOGOS | Relatório de Bombardeio, PDF escaneado | Docling |
| INTEL | informe de inteligência, PDF escaneado | tesseract → modelo de linguagem |
| VOZ | mensagem de rádio `.wav` | faster-whisper → modelo de linguagem |

Todo evento em que alguma IA participou traz a cadeia de ferramentas na coluna
`EVENTO.EXTRACAO_MODELO_NOME` (ex.: `ollama qwen3.5:4b <- faster-whisper 1.1.0 medium-int8`), para
quem consulta saber que precisa conferir.

---

## Pré-requisitos

| Item | Observação |
|---|---|
| Docker com Compose v2 | `docker compose` (com espaço). Testado no Docker Desktop para Linux |
| Disco | **~35 GB** para as imagens (Airflow ~9 GB, OpenMetadata ~9 GB, Docling ~8 GB...) |
| Memória para o Docker | **16 GB**. Testado com 12 CPUs |
| Internet na primeira subida | imagens, modelo do Whisper (baixado na construção) e vozes do gerador |
| Python 3.10+ na máquina | só para os geradores de dados sintéticos |
| **Ollama** na máquina | https://ollama.com, com o modelo: `ollama pull qwen3.5:4b` |
| Placa de vídeo NVIDIA | **opcional**. O Ollama a usa sozinho; sem ela tudo roda, só que mais devagar |

**Por que o Ollama fica fora do Docker?** No Docker Desktop, o contêiner não enxerga a placa de
vídeo. Como servidor na máquina, o Ollama usa a GPU e as DAGs falam com ele por HTTP, pelo endereço
`host.docker.internal:11434`.

> **Docker Engine no Linux (sem Desktop):** o Ollama escuta só em `127.0.0.1` e recusa o contêiner.
> Libere-o com `sudo systemctl edit ollama`, acrescentando
> `[Service]` / `Environment="OLLAMA_HOST=0.0.0.0"`, e depois `sudo systemctl restart ollama`.

---

## Primeira subida, passo a passo

### 1. Configuração

```bash
cp .env.example .env
cp infra/openlineage/openlineage.yml.example infra/openlineage/openlineage.yml
```

Faça a segunda cópia **antes** do passo 2. Se o arquivo não existir, o Docker cria uma *pasta* com
esse nome no lugar dele, e o Airflow e o Spark passam a enxergar uma pasta onde esperavam a
configuração da linhagem.

### 2. Subir a stack

```bash
docker compose up -d --build     # a primeira vez constrói as imagens do Airflow e do Spark (demora)
docker compose ps                # esperar todos "running" / "healthy" (o OpenMetadata leva alguns minutos)
```

> **Nunca use `docker compose restart`**: ele não reaplica variáveis de ambiente.
> Depois de mudar `.env` ou `docker-compose.yml`, use sempre `docker compose up -d`.

### 3. Criar as tabelas e carregar as referências

As tabelas saem do modelo canônico. Os dois comandos abaixo rodam o job Spark uma vez com `--ddl`
(cria as tabelas Bronze, Silver e Gold) e uma vez com `--seeds` (carrega as tabelas `REF_*` a partir de
`canonico/seeds/*.csv`):

```bash
for etapa in --ddl --seeds; do
  docker exec -e MODELO_CANONICO=/opt/canonico/modelo_canonico.yaml dlh_spark_master sh -c "
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      --conf spark.cores.max=1 --conf spark.executor.memory=4g \
      --conf spark.sql.shuffle.partitions=4 \
      --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      --conf spark.hadoop.fs.s3a.access.key=\$MINIO_ROOT_USER \
      --conf spark.hadoop.fs.s3a.secret.key=\$MINIO_ROOT_PASSWORD \
      /opt/spark-jobs/canonico_para_silver.py $etapa"
done
```

### 4. Gerar os dados sintéticos e enviar para o MinIO

Os geradores rodam na máquina, fora do Docker, com um ambiente Python próprio:

```bash
python3 -m venv venv-geradores
./venv-geradores/bin/pip install -r requirements-geradores.txt
./venv-geradores/bin/python scripts/geradores/gerar_tudo.py        # --escala 0.1 para um teste rápido
./venv-geradores/bin/python scripts/geradores/enviar_landing.py    # dados_sinteticos/landing/ -> s3://lakehouse/landing/
```

A saída fica em `dados_sinteticos/` (fora do git): `landing/` é o que vai para o MinIO e `verdade/`
é o gabarito (fatos do cenário, defeitos plantados de propósito, frase de cada áudio). Os
`canonico/seeds/*.csv` já vêm no repositório; `gerar_seeds.py` só é necessário se o cenário mudar.

### 5. Governança (fazer ANTES de rodar o pipeline, para a linhagem aparecer)

A seta de linhagem só é criada entre tabelas que o OpenMetadata já conhece. Por isso, o serviço e
o catálogo vêm antes das DAGs 1 a 3.

1. **Token do robô de ingestão.** Siga os comandos no fim de
   `infra/openlineage/openlineage.yml.example`. Grave o token em **dois** lugares:
   `OM_INGESTION_BOT_JWT` no `.env` e `apiKey` no `infra/openlineage/openlineage.yml`. Depois rode
   `docker compose up -d`.
2. **Serviço Trino no OpenMetadata** (http://localhost:8585, `admin@open-metadata.org` / `admin`):
   *Settings → Services → Databases → Add New Service → Trino*, com:
   - nome: `trino_lakehouse` (exatamente este: as DAGs o procuram pelo nome);
   - host: `trino:8090`;
   - usuário: `admin`;
   - catálogo: `iceberg`.
3. **Catalogar as tabelas:** no Airflow (http://localhost:8080, `admin` / `admin`), despausar e
   disparar a DAG `trino_lakehouse_metadata`.
4. **Testes de qualidade e glossário** (opcional, rodam na máquina, só com biblioteca padrão):
   ```bash
   python3 scripts/testes_qualidade.py
   python3 scripts/governanca/provisionar_governanca.py --tudo
   ```

### 6. Rodar o pipeline

No Airflow (http://localhost:8080, `admin` / `admin`), as DAGs **nascem pausadas**. Despause
`1_ingestao`, `2_bronze_extracao`, `3_silver_evento`, `4_gold_visoes` e `5_governanca` (botão ao lado do nome, ou
`airflow dags unpause <nome>`) e dispare **uma de cada vez, em ordem**, esperando a anterior terminar:

```bash
docker exec dlh_airflow_webserver airflow dags trigger 1_ingestao
docker exec dlh_airflow_webserver airflow dags trigger 2_bronze_extracao   # a mais longa, ver abaixo
docker exec dlh_airflow_webserver airflow dags trigger 3_silver_evento     # ~3 min
docker exec dlh_airflow_webserver airflow dags trigger 4_gold_visoes
docker exec dlh_airflow_webserver airflow dags trigger 5_governanca
```

Cada DAG começa conferindo se há algo pendente. Se não houver, vai para `nada_a_fazer`, e por isso
dispará-la de novo é seguro.

**Tempo da DAG 2.** A transcrição dos 150 áudios leva ~14 min e as tabelas dos 78 PDFs pelo Docling,
~6 min, ambas em CPU. O modelo de linguagem responde ~480 pedidos: **com GPU, ~1 s cada (minutos);
só em CPU, ~18 s cada (algumas horas)**. A DAG grava a cada 50 itens, então uma queda no meio não
perde o que já foi feito.

Para conferir o resultado no Trino:

```bash
docker exec dlh_trino trino --server localhost:8090 --execute \
  "SELECT sistema_origem_cod, modalidade_origem_cod, count(*) FROM iceberg.silver.evento GROUP BY 1, 2 ORDER BY 1"
```

---

## Portas

Todas publicadas só em `127.0.0.1` (a máquina local).

| Serviço | Endereço |
|---|---|
| Airflow | http://localhost:8080 |
| OpenMetadata | http://localhost:8585 |
| Trino | http://localhost:8090 |
| MinIO Console / API S3 | http://localhost:9001 · http://localhost:9000 |
| Spark Master UI / Worker UI | http://localhost:8081 · http://localhost:8082 |
| Docling (API + documentação em `/docs`) | http://localhost:5001 |
| Ollama (na máquina, fora do compose) | http://localhost:11434 |
| Hive Metastore · PostgreSQL · Elasticsearch | 9083 · 5432 · 9200 |

Credenciais de desenvolvimento local: `.env.example`.

---

## DAGs

| DAG | O que faz |
|---|---|
| `1_ingestao` | cataloga o que chegou em `landing/` em `RECEPCAO_BRUTA` e `ARQUIVO` |
| `2_bronze_extracao` | lê o conteúdo: planilha → OCR → tabela (Docling) → voz → texto → relato → informe |
| `3_silver_evento` | uma tarefa por receita do YAML; escreve `EVENTO`, `SITUACAO_UNIDADE` e `MEDIDA_COORDENACAO` |
| `4_gold_visoes` | cinco tarefas em série: `pitcic` → `problema_dados` → `ppcot` → `coc` → `amc`; cada uma reescreve sua tabela inteira. Nomes de coluna pelo IR 14-06; valores das células com as siglas do MD33-M-02 |
| `5_governanca` | OpenMetadata: metadados → perfil → qualidade → amostra |
| `trino_lakehouse_metadata` | cataloga as tabelas no OpenMetadata; rodar sempre que criar tabela nova |
| `dag_iceberg_maintenance` | manutenção das tabelas Iceberg: expira snapshot velho, apaga arquivo órfão, reagrupa manifestos, compacta arquivos pequenos. Diária às 02:00; a lista de tabelas sai do modelo canônico |

---

## Operação do dia a dia

- **Mudou uma regra no YAML?** A DAG 3 só processa o que está *pendente* (registro sem evento), então
  não reaplica a regra ao que já está na Silver. Reprocesse a receita à mão, com o mesmo comando do
  passo 3, trocando `$etapa` por `--fonte <FONTE> --tipo <receita>`:

  | Fonte | Receitas |
  |---|---|
  | `C2_A` | `posicao`, `mcc` |
  | `C2_B` | `relato`, `incidente` |
  | `RELPER` | `situacao`, `situacao_digitalizada` |
  | `VOZ` · `INTEL` · `FOGOS` | `transcricao` · `informe` · `relatorio_bombardeio` |

  A gravação na Silver atualiza o que já existe, pela chave do evento, sem duplicar.
- **Mudou um prompt?** Dê um código de versão novo na DAG 2 (ex.: `voz-v2`). A interpretação antiga
  fica na Bronze, a nova entra ao lado e a Silver usa a mais recente. A DAG **para com erro** se o
  mesmo código aparecer com texto diferente (ver `AJUSTE_EXTRACAO` e `canonico/prompts.md`).
- **A Bronze só recebe linhas novas.** Nada de `DELETE`/`UPDATE` nela.
- **Editou um arquivo e o contêiner não viu a mudança?** No Docker Desktop, o editor pode trocar o
  arquivo por um novo, e o contêiner continua enxergando o antigo. Confira o tamanho dentro e fora
  (`stat -c %s`). Se forem diferentes, regrave no lugar: `cp arq /tmp/x && cat /tmp/x > arq`.
- **Recomeçar do zero:** `docker compose down -v` apaga **tudo**: dados, catálogo, Airflow e
  OpenMetadata. Depois é preciso refazer os passos 2 a 6, inclusive o token novo do robô.

---

## Estrutura do repositório

```
docker-compose.yml          a stack inteira (comentada)
.env.example                variáveis de ambiente (copiar para .env, que nunca é commitado)
canonico/
  modelo_canonico.yaml      o modelo canônico: entidades, domínios, receitas de cada fonte
  modelo_canonico.html      diagramas e um exemplo de ponta a ponta
  prompts.md                o texto de cada versão de prompt enviado aos modelos de IA
  seeds/                    tabelas de referência (operação, unidades, lugares)
  ir_14_06/                 glossário e abreviaturas (IR 14-06) para o OpenMetadata
airflow/dags/               DAGs numeradas pela ordem do fluxo + helpers/lineage_emitter.py
spark/jobs/
  ingestao_bronze.py        landing/ -> RECEPCAO_BRUTA + ARQUIVO
  canonico_para_silver.py   --ddl, --seeds e o de/para Bronze -> Silver
  gold_visoes.py            as visões da Gold (Silver -> Gold)
  iceberg_maintenance.py    manutenção das tabelas (expire, orphan, manifests, compaction)
scripts/
  geradores/                geradores dos dados sintéticos, um por fonte
  governanca/               provisionamento do glossário no OpenMetadata
  testes_qualidade.py       os 7 testes de qualidade da Bronze
  medir_acerto_relato.py    acerto do modelo de linguagem contra o gabarito
infra/                      Dockerfiles e configuração de Airflow, Spark, Trino, MinIO, OpenLineage
ACHADOS.md                  o que custou tempo para descobrir, com números de acerto e erros da IA
relatorios/                 guias do OpenMetadata e histórico (os relatórios comparativos estão superados)
```

Arquivos da fase anterior, mantidos só como histórico: `scripts/gerar_dados.py`,
`scripts/coletar_metricas_gqm.py` e `setup_venv.sh` — os três leem tabelas (`bronze.dados`,
`silver.gps`, `gold.coc`) que não existem mais no modelo canônico v3.
