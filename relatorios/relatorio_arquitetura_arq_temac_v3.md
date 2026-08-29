# Arquitetura de Dados para o Sistema de Comando e Controle do Exercito Brasileiro
## Relatorio Tecnico — Projeto ARQ_TEMAC

**Versao:** 3.0
**Data:** 25 de abril de 2026
**Contexto:** Dissertacao de mestrado — Prova de Conceito (PoC)
**Alteracoes em relacao a v2:** Inclusao do framework de avaliacao com 7 indicadores praticos, justificativa metodologica do baseline PostgreSQL como representante do paradigma relacional, e alinhamento com a metodologia GQM (Goal-Question-Metric).

---

## 1. Sintese do Problema

O sistema de Comando e Controle (C2) do Exercito Brasileiro opera em um ambiente com caracteristicas muito especificas que tornam as solucoes convencionais de dados inadequadas:

- **Heterogeneidade extrema de dados:** posicionamento GPS, SITREPs textuais, imagens de drone, ordens em PDF, metadados de sensores.
- **Conectividade intermitente e assimetrica:** o comandante de batalhao depende de radios HF/VHF com banda restrita e janelas de oportunidade; o comandante de brigada tem internet por satelite com qualidade variavel.
- **Fluxo bidirecional:** os dados sobem (batalhoes -> brigada) de forma caotica e heterogenea; as ordens descem (brigada -> batalhoes) de forma estruturada e formalizada.
- **Necessidade de fusao analitica:** o Cenario Operacional Comum (COC) e construido no nivel brigada integrando informacoes de todos os batalhoes subordinados.
- **Paradigma atual:** os sistemas de C2 do EB utilizam bancos de dados relacionais para persistencia dos dados operacionais, conforme e padrao em aplicacoes de gestao militar. As especificacoes tecnicas internas desses sistemas nao sao de dominio publico.

A dissertacao investiga se uma arquitetura Lakehouse Medallion (Bronze -> Silver -> Gold), implementada com tecnologias open-source, oferece vantagens mensuraveis sobre o paradigma relacional para esse contexto.

---

## 2. Decisao Arquitetural Central: Um Lakehouse Centralizado na Brigada

### 2.1 O que foi considerado e descartado

**Opcao descartada: Dois Lakehouses (edge + central)**

A alternativa de instalar um Lakehouse completo em cada nivel (batalhao e brigada) foi descartada para a PoC pelos seguintes motivos:

- Duplica complexidade operacional: dois catalogos, dois registros de schema, logica de *conflict resolution* na sincronizacao.
- Requer coordenacao de *schema evolution* entre instancias distintas.
- O EB nao dispoe de pessoal tecnico suficiente para sustentar multiplas instancias em condicoes de campo.
- Para fins academicos, o valor demonstravel esta na capacidade analitica do Lakehouse, nao na solucao do problema de distribuicao.

**Opcao adotada: Um Lakehouse centralizado no nivel brigada**

Esta escolha e justificada por tres perspectivas convergentes:

- **Arquitetural:** um unico catalogo, schema registry e ponto de *time travel*; governanca simples; alinhada ao conceito Lakehouse unificado de Zaharia et al. (2021).
- **Operacional (C2):** o posto de comando da brigada e o **ponto de fusao doutrinario** onde o COC e construido — modelar o Lakehouse nesse nivel e modelar onde a consciencia situacional agregada ocorre (Verhoosel et al., 2020).
- **Doutrinaria:** o EB70-MC-10.205 define o Centro de Operacoes da brigada como o principal orgao de integracao de informacoes do campo de batalha; o EB70-MC-10.211 (PPCOT) e o MC 3.11-10 confirmam que o fluxo de sincronizacao ascendente (batalhao -> brigada) e o fluxo critico a ser gerenciado.

### 2.2 Por que a brigada e o escalao ideal para a PoC

- E o menor Grande Comando com estado-maior completo.
- Comanda tipicamente 3 a 5 batalhoes — volume gerenciavel de fontes de dados para uma PoC.
- O MC 3.11-10 trata especificamente das comunicacoes nesse nivel, fornecendo base doutrinaria para as restricoes modeladas.

### 2.3 Por que Apache Iceberg e nao Delta Lake

**Interoperabilidade de engines:**
- Iceberg foi projetado como formato **agnostico de engine**. Spark, Trino, Flink e DuckDB possuem conectores nativos de primeira classe.
- O conector Trino-Iceberg e **nativo e de primeira classe**, com suporte completo a *predicate pushdown*, *partition pruning* e operacoes de escrita.

**Evolucao de schema e particao:**
- Iceberg suporta **partition evolution** — e possivel mudar o esquema de particionamento sem reescrever os dados existentes.
- Iceberg suporta **hidden partitioning** — as queries nao precisam saber como a tabela esta particionada.
- *Schema evolution* no Iceberg e totalmente compativel com Avro evolution rules: adicao de colunas, renomeacao, reordenacao e promocao de tipo — tudo sem reescrita.

**Catalogo e metadados:**
- Iceberg usa um **catalogo central** (REST Catalog, Hive Metastore, ou Nessie) que registra o estado de cada tabela via *metadata files* e *manifest lists*.
- Cada *snapshot* Iceberg e imutavel e enderecavel, proporcionando *time travel* e *rollback* nativos.

**Comunidade e governanca do projeto:**
- Apache Iceberg e um projeto **Apache Software Foundation** com governanca aberta e neutra em relacao a fornecedores.
- Para uma dissertacao que exige stack 100% open-source, a governanca ASF oferece garantias mais fortes de neutralidade.

**Trade-off aceito:**
- Delta Lake tem *merge-on-read* mais otimizado para upsert frequente. No contexto desta PoC, os dados sao majoritariamente *append-only* (Bronze e ingestao pura), o que favorece o Iceberg.

---

## 3. Arquitetura Proposta

### 3.1 Visao geral

```
+-------------------------------------------------------------------+
|                    POSTO DE COMANDO — BRIGADA                      |
|                                                                    |
|  +----------------+    +----------------------------------------+  |
|  |  GERADOR DE    |    |         LAKEHOUSE MEDALLION             |  |
|  |    DADOS       |    |                                        |  |
|  |  SINTETICOS    |    |  +---------+  +--------+  +--------+  |  |
|  |                |--->|  | BRONZE  |->| SILVER |->|  GOLD  |  |  |
|  |  Simula:       |    |  |  MinIO  |  | Spark  |  | Trino  |  |  |
|  |  - Batalhao 1  |    |  | Iceberg |  |Iceberg |  |Iceberg |  |  |
|  |  - Batalhao 2  |    |  +---------+  +--------+  +--------+  |  |
|  |  - Batalhao 3  |    |                                        |  |
|  |                |    |  +------------------------------------+|  |
|  |  Com atrasos   |    |  |     CATALOGO ICEBERG               ||  |
|  |  variaveis     |    |  |  REST Catalog / Hive Metastore     ||  |
|  +----------------+    |  +------------------------------------+|  |
|                        |                                        |  |
|  +----------------+    |  +------------------------------------+|  |
|  |  POSTGRESQL    |    |  |         ORQUESTRACAO               ||  |
|  |   BASELINE     |    |  |           Airflow                  ||  |
|  |  (paradigma    |    |  +------------------------------------+|  |
|  |  relacional)   |    |                                        |  |
|  +----------------+    |  +------------------------------------+|  |
|                        |  |         GOVERNANCA                 ||  |
|                        |  |  OpenMetadata + PostgreSQL         ||  |
|                        |  +------------------------------------+|  |
|                        +----------------------------------------+  |
+-------------------------------------------------------------------+
```

### 3.2 Camadas da Arquitetura Medallion

#### Camada Bronze — Landing Zone (MinIO + Apache Iceberg)

**Responsabilidade:** Ingestao e armazenamento dos dados brutos exatamente como chegaram, sem transformacao.

**Caracteristicas:**
- Dados imutaveis; nada e deletado, apenas adicionado (*append-only*).
- Schema-on-read: aceita qualquer estrutura sem rejeicao.
- Cada arquivo/registro recebe metadados de proveniencia no momento da ingestao: `timestamp_geracao`, `timestamp_chegada`, `batalhao_origem`, `tipo_dado`, `id_lote`.
- **Hidden partitioning** do Iceberg: a tabela e particionada internamente por `days(timestamp_chegada)` e `batalhao_origem`, mas as queries nao precisam incluir filtros explicitos de particao.
- Formato de arquivo: **Apache Parquet** (formato de dados fisico do Iceberg).
- Cada ingestao gera um novo **snapshot** Iceberg, permitindo *time travel* para qualquer estado anterior da tabela.

**Tipos de dados ingeridos:**

| Tipo | Formato | Frequencia simulada | Atraso simulado |
|---|---|---|---|
| Posicionamento GPS | JSON -> Parquet | A cada 30s (quando ha link) | 0-45 min |
| SITREP | JSON/texto -> Parquet | A cada 2h | 0-3h |
| Metadados de drone | JSON -> Parquet | A cada 10 min | 0-60 min |

**Por que batch e nao streaming?**

O MC 3.11-10 documenta que a transmissao de dados taticos no nivel batalhao ocorre em **janelas de oportunidade**, nao continuamente. O batch e a escolha arquitetural correta porque espelha o comportamento real do sistema: os dados chegam em **lotes** quando a janela de conectividade abre.

#### Camada Silver — Refinamento (Apache Spark + Iceberg)

**Responsabilidade:** Limpeza, normalizacao, deduplicacao e enriquecimento dos dados.

**Transformacoes aplicadas:**
- **Deduplicacao:** registros duplicados identificados e colapsados via `MERGE INTO`.
- **Normalizacao de schema:** dados de diferentes batalhoes harmonizados em schema canonico. O Iceberg suporta **schema evolution** nativa.
- **Enriquecimento de proveniencia:** calculo de `latencia_ingestao` = `timestamp_chegada` - `timestamp_geracao`; flag de `fora_de_ordem` para registros com sequencia temporal invertida.
- **Joins temporais:** correlacao entre eventos (posicionamento + SITREP do mesmo batalhao na mesma janela temporal).
- **Validacao de qualidade:** campos obrigatorios, ranges validos de coordenadas, timestamps coerentes.

**Tecnologia:** Apache Spark com conector Iceberg nativo. Jobs orquestrados pelo Airflow.

#### Camada Gold — Consumo Analitico (Trino + Iceberg)

**Responsabilidade:** Agregar, sumarizar e disponibilizar visoes prontas para consumo decisorio.

**Visoes construidas:**

| Visao | Descricao | Uso no COC |
|---|---|---|
| `posicionamento_atual` | Ultima posicao conhecida de cada subunidade | COP tatico |
| `sitrep_consolidado` | SITREPs agregados por batalhao, janela temporal | Situacao do campo |
| `latencia_por_batalhao` | Distribuicao de latencia de ingestao por fonte | Diagnostico de conectividade |
| `cobertura_temporal` | Periodos sem dados por subunidade (gaps de conectividade) | Identificacao de pontos cegos |
| `atividade_sensores` | Agregacao de metadados de drone por area geografica | Reconhecimento |

**Tecnologia:** Trino com conector Iceberg nativo. Trino le diretamente os *manifest files* do Iceberg no MinIO, aplicando *predicate pushdown* e *partition pruning* automaticamente.

**Time travel no Gold:** O comandante pode consultar o estado do COC em qualquer ponto no tempo anterior usando `SELECT * FROM gold.posicionamento_atual FOR TIMESTAMP AS OF TIMESTAMP '2026-04-24 14:00:00'`.

### 3.3 Catalogo Iceberg

**Recomendacao para a PoC:** Hive Metastore com PostgreSQL como backend. Reutiliza o PostgreSQL ja presente na stack e tem suporte nativo tanto no Spark quanto no Trino.

### 3.4 Orquestracao — Apache Airflow

**DAGs propostas:**

1. **`dag_ingestao_bronze`:** Monitora diretorio de entrada, converte JSON para Parquet e registra como tabela Iceberg no MinIO com metadados de proveniencia.
2. **`dag_silver_transform`:** Executa jobs Spark de normalizacao sobre tabelas Iceberg. Agendada a cada 15 minutos ou acionada por chegada de novos dados no Bronze.
3. **`dag_gold_refresh`:** Atualiza visoes Gold apos cada execucao bem-sucedida do Silver. Pode incluir *compaction* de *small files* no Iceberg.
4. **`dag_baseline_sync`:** Carrega os mesmos dados sinteticos no PostgreSQL baseline para comparacao de metricas.
5. **`dag_iceberg_maintenance`:** Executa periodicamente *expire snapshots*, *remove orphan files*, *rewrite manifests*.

### 3.5 Governanca — OpenMetadata + Catalogo Iceberg

**Complementaridade:**
- O catalogo Iceberg gerencia o *estado tecnico* das tabelas (snapshots, manifests, schemas, particoes).
- O OpenMetadata gerencia o *estado de negocio* (descricoes, owners, tags, linhagem, qualidade).

**Alinhamento bibliografico:**
- goldMEDAL (Scholly et al., 2021) — referencia conceitual para metadados.
- MEDAL (Sawadogo et al., 2019) — linhagem e rastreabilidade.
- DLAF (Giebler et al., 2021a) — framework arquitetural de referencia.

### 3.6 Stack tecnologica completa

| Componente | Tecnologia | Papel |
|---|---|---|
| Armazenamento de objetos | MinIO | Repositorio central para todas as zonas Medallion |
| Formato de tabela | Apache Iceberg | ACID, time travel, schema/partition evolution, hidden partitioning |
| Formato de arquivo fisico | Apache Parquet | Armazenamento colunar eficiente (gerenciado pelo Iceberg) |
| Catalogo de tabelas | Hive Metastore (PostgreSQL backend) | Registro de estado das tabelas Iceberg |
| Processamento | Apache Spark | Transformacoes Bronze->Silver->Gold |
| Engine de consulta | Trino | Consultas analiticas ad hoc sobre tabelas Iceberg |
| Orquestracao | Apache Airflow | Agendamento e monitoramento de pipelines |
| Catalogo e governanca | OpenMetadata | Metadados de negocio, linhagem, qualidade |
| Baseline relacional | PostgreSQL | Representante do paradigma relacional para comparacao |
| Infraestrutura | Docker Compose | Ambiente de laboratorio; escala de PoC |

---

## 4. Gerador de Dados Sinteticos

### 4.1 O que simular

**Tres batalhoes com perfis distintos de conectividade:**

- **Batalhao 1 (conectividade razoavel):** envia lotes a cada 15 minutos, atraso medio de 5 minutos, raro gap.
- **Batalhao 2 (conectividade degradada):** janelas de 30 minutos, seguidas de silencios de 1-3 horas. Atrasos variaveis.
- **Batalhao 3 (conectividade minima):** apenas 3-4 janelas ao dia, com atrasos de ate 6 horas. Dados chegam fora de ordem.

**Por lote, o gerador produz:**
- N registros de posicionamento GPS (uma entrada por subunidade, com coordenadas incrementais simulando movimento).
- 0-1 SITREPs (texto semiestruturado com campos padronizados: hora, situacao, baixas, necessidades).
- 0-1 pacotes de metadados de sensor (se o "drone" estava ativo no periodo).

**Metadados obrigatorios em cada registro:**
```json
{
  "id_registro": "uuid",
  "batalhao_origem": "1BIS",
  "tipo_dado": "gps|sitrep|sensor",
  "timestamp_geracao": "2026-04-24T08:15:00Z",
  "timestamp_chegada": "2026-04-24T09:02:47Z",
  "id_lote": "lote_0042"
}
```

### 4.2 Timestamps e seu significado operacional

Cada registro carrega dois timestamps explicitos e o Iceberg adiciona um terceiro implicito:

| Timestamp | Origem | Significado operacional |
|---|---|---|
| `timestamp_geracao` | Gerador de dados | Momento em que o dado foi produzido no batalhao |
| `timestamp_chegada` | Momento de recepcao na brigada | Quando o lote chegou ao posto de comando |
| `snapshot_timestamp` | Iceberg (automatico) | Quando o dado se tornou disponivel para consulta |

A diferenca entre esses timestamps e o que permite medir as latencias do pipeline e, por extensao, a **latencia decisoria** do COC.

---

## 5. Baseline PostgreSQL — Justificativa Metodologica

### 5.1 Por que PostgreSQL

O baseline relacional (PostgreSQL) **nao representa um sistema especifico em uso no EB**. Ele representa o **paradigma relacional** como classe de solucao. A escolha se justifica por:

1. **Alinhamento com a stack:** PostgreSQL e a solucao relacional open-source mais madura, coerente com a restricao de tecnologias abertas do projeto.
2. **Reuso de infraestrutura:** PostgreSQL ja compoe a PoC como backend do catalogo Iceberg (Hive Metastore), minimizando a complexidade do ambiente.
3. **Comparacao paradigmatica:** a dissertacao compara dois paradigmas de arquitetura de dados — relacional vs Lakehouse — nao dois produtos. PostgreSQL e a instancia do primeiro; Iceberg+Trino e a instancia do segundo. Essa abordagem segue Nambiar e Mundra (2022), que comparam Data Warehouse vs Data Lake sem vincular a comparacao a um sistema legado especifico.
4. **Independencia de acesso:** as especificacoes tecnicas dos sistemas internos de C2 do EB nao sao de dominio publico. Utilizar PostgreSQL como baseline generico evita afirmacoes nao verificaveis sobre sistemas em producao.

### 5.2 Equidade da comparacao

Para que a comparacao seja academicamente valida, o baseline PostgreSQL recebe **engenharia equivalente** ao Lakehouse:

- Tabelas separadas por tipo de dado (GPS, SITREP, sensores), com indices otimizados.
- Views de fusao analitica equivalentes as visoes Gold do Lakehouse (ex: `v_posicionamento_atual`, `v_sitrep_consolidado`).
- Os mesmos dados sinteticos sao carregados em ambos os ambientes pela DAG `dag_baseline_sync`.
- As mesmas queries de benchmark sao executadas em ambos.

A comparacao nao e "Lakehouse engenheirado vs PostgreSQL cru" — e "Lakehouse engenheirado vs PostgreSQL engenheirado".

---

## 6. Framework de Avaliacao

### 6.1 Fundamentacao metodologica

A avaliacao da PoC segue a logica da metodologia **Goal-Question-Metric (GQM)** de Basili et al. [204], utilizada tambem por DaNova (PPCA/UnB) na validacao de uma arquitetura de integracao de dados de simulacao de combate do EB. Os atributos de qualidade de referencia sao os da **ISO/IEC 25010** [126], conforme Richards e Ford [125].

O GQM estrutura a avaliacao em tres niveis:

1. **Goal (Objetivo):** o que se quer demonstrar com a PoC.
2. **Question (Questao):** perguntas que, respondidas, confirmam ou refutam o objetivo.
3. **Metric (Metrica):** medicoes concretas que respondem cada questao.

Os indicadores definidos abaixo foram selecionados por serem **praticos e mensuraveis** em ambiente de laboratorio (Docker Compose, maquina local, dados sinteticos), sem necessidade de infraestrutura de monitoramento adicional. Todos sao coletaveis com queries SQL e comandos de terminal.

### 6.2 Os 7 Indicadores

#### Indicador 1 — Registros Preservados (Consistencia do Pipeline)

| Item | Descricao |
|---|---|
| **O que mede** | Se o pipeline Medallion preserva todos os registros da ingestao ate o consumo |
| **Atributo de qualidade** | Confiabilidade |
| **Questao GQM** | O pipeline perde dados ao longo das transformacoes Bronze->Silver->Gold? |
| **Como medir** | `SELECT COUNT(*) FROM bronze.dados`, `SELECT COUNT(*) FROM silver.dados`, `SELECT COUNT(*) FROM gold.posicionamento_atual` (ajustado por tipo e agregacao) |
| **Resultado esperado** | Nenhum registro perdido entre Bronze e Silver. Gold pode ter contagem diferente por ser agregada, mas todo registro Bronze deve ser rastreavel |
| **Referencia DaNova** | Taxa de Consistencia = 100%, Taxa de Rastreamento = 100% |

#### Indicador 2 — Latencia de Ingestao

| Item | Descricao |
|---|---|
| **O que mede** | Tempo entre a chegada do lote de dados e sua disponibilidade na camada Bronze |
| **Atributo de qualidade** | Desempenho |
| **Questao GQM** | O Lakehouse ingere os dados com rapidez? |
| **Como medir** | `SELECT AVG(timestamp_disponivel_bronze - timestamp_chegada) FROM bronze.dados` — o `timestamp_disponivel_bronze` e o `snapshot_timestamp` do Iceberg ou o horario de conclusao da DAG de ingestao |
| **Onde medir** | Lakehouse e PostgreSQL (mesma medicao: tempo entre chegada e commit) |
| **Significado operacional** | Quanto tempo o dado do batalhao leva para entrar no sistema da brigada |
| **Referencia DaNova** | Tempo Medio de Upload = 0,533 ms (rede local, sem simulacao de intermitencia) |

#### Indicador 3 — Latencia Ponta-a-Ponta do Pipeline

| Item | Descricao |
|---|---|
| **O que mede** | Tempo total desde a geracao do dado no batalhao ate sua disponibilidade na camada Gold |
| **Atributo de qualidade** | Desempenho |
| **Questao GQM** | Quanto tempo leva para o dado do batalhao aparecer no COC da brigada? |
| **Como medir** | `SELECT AVG(timestamp_disponivel_gold - timestamp_geracao) FROM gold.posicionamento_atual GROUP BY batalhao_origem` |
| **Onde medir** | Lakehouse e PostgreSQL (mesma logica, usando timestamp de atualizacao da view) |
| **Significado operacional** | Esta e a metrica mais relevante para o C2: ela traduz a **latencia decisoria**. Medida por batalhao, revela o impacto da conectividade na consciencia situacional. O MC 3.11-10 preve que o Batalhao 3 tera latencia muito maior que o Batalhao 1 — o indicador **documenta e quantifica** essa degradacao |

#### Indicador 4 — Throughput por Zona

| Item | Descricao |
|---|---|
| **O que mede** | Registros processados por segundo em cada transicao do pipeline |
| **Atributo de qualidade** | Desempenho |
| **Questao GQM** | O pipeline processa os dados com velocidade adequada? |
| **Como medir** | `registros_processados / tempo_execucao_job`. O Airflow registra o tempo de cada DAG run. O `COUNT(*)` por zona fornece o numerador |
| **Desdobramentos** | Throughput Bronze->Silver (job Spark) e Throughput Silver->Gold (job Spark/Trino) |
| **Onde medir** | Lakehouse (Spark) e PostgreSQL (tempo de INSERT + UPDATE das views) |
| **Referencia DaNova** | Taxa Media de Geracao DIS = 552,29 msg/s; Protobuf = 84,79 msg/s |

#### Indicador 5 — Tempo de Resposta Analitico Comparado

| Item | Descricao |
|---|---|
| **O que mede** | Tempo de execucao das mesmas consultas analiticas no Lakehouse (Trino) e no PostgreSQL |
| **Atributo de qualidade** | Desempenho |
| **Questao GQM** | O Lakehouse e mais rapido que o PostgreSQL para consultas analiticas de fusao multi-fonte? |
| **Como medir** | Definir 5 queries de benchmark (ver secao 6.3). Executar cada query 5 vezes em cada ambiente (com warm-up). Registrar media e desvio padrao |
| **Onde medir** | Trino sobre Iceberg vs PostgreSQL |
| **Significado operacional** | Responde se o Lakehouse entrega a imagem do campo de batalha mais rapido. Se sim, o PPCOT (EB70-MC-10.211) ganha velocidade na fase de analise |

#### Indicador 6 — Taxa de Compressao

| Item | Descricao |
|---|---|
| **O que mede** | Eficiencia de armazenamento ao longo do pipeline e entre paradigmas |
| **Atributo de qualidade** | Escalabilidade |
| **Questao GQM** | O Lakehouse ocupa menos espaco que o PostgreSQL para os mesmos dados? |
| **Como medir** | `du -sh` nos buckets MinIO (Bronze e Warehouse) e no diretorio de dados do PostgreSQL. Razao = tamanho_bronze / tamanho_gold e tamanho_postgres / tamanho_gold |
| **Referencia DaNova** | Taxa de Compressao = 24,48x (Bronze -> Warehouse, formato Iceberg) |

#### Indicador 7 — Schema Evolution

| Item | Descricao |
|---|---|
| **O que mede** | Capacidade do Lakehouse de absorver mudancas de schema sem quebrar o pipeline |
| **Atributo de qualidade** | Manutenibilidade |
| **Questao GQM** | O Lakehouse absorve novos campos de dados sem intervencao manual? |
| **Como medir** | Teste binario: (1) adicionar um campo novo no gerador de dados (ex: `temperatura` no SITREP do Batalhao 2), (2) rodar o pipeline, (3) verificar se a Bronze aceita, se a Silver normaliza, e se a Gold continua funcionando. No PostgreSQL, repetir o teste — o que exigira `ALTER TABLE` |
| **Resultado esperado** | Iceberg: schema evolution automatica sem quebra. PostgreSQL: requer intervencao manual (`ALTER TABLE ADD COLUMN`) |

### 6.3 Queries de Benchmark

As seguintes queries sao executadas em ambos os ambientes (Trino/Iceberg e PostgreSQL) para o Indicador 5:

| # | Query | Complexidade | Produto do COC |
|---|---|---|---|
| Q1 | Ultima posicao conhecida de cada subunidade | Simples (GROUP BY + MAX) | COP tatico |
| Q2 | SITREPs das ultimas 4 horas, agrupados por batalhao | Filtro temporal + agregacao | Quadro de situacao |
| Q3 | Fusao posicao + SITREP + sensor na mesma janela temporal (1h) por batalhao | JOIN temporal multi-fonte | COC completo |
| Q4 | Distribuicao de latencia de ingestao por batalhao (percentis p50, p90, p99) | Funcoes analiticas | Diagnostico de conectividade |
| Q5 | Gaps de cobertura: periodos > 1h sem dados por batalhao | Window functions | Identificacao de pontos cegos |

Estas queries foram desenhadas para representar consultas reais que o estado-maior faria ao construir o COC. A Q3 (fusao multi-fonte) e a mais exigente e a mais relevante — e nela que o Lakehouse deve demonstrar vantagem, pois o paradigma relacional requer JOINs entre tabelas separadas enquanto o Lakehouse pode operar sobre dados ja pre-fundidos na Gold.

### 6.4 Resumo dos indicadores e onde medir

| # | Indicador | Lakehouse | PostgreSQL | Comparativo? |
|---|---|---|---|---|
| 1 | Registros preservados | Sim | Sim | Sim — ambos devem preservar 100% |
| 2 | Latencia de ingestao | Sim | Sim | Sim — tempo de commit vs snapshot |
| 3 | Latencia ponta-a-ponta | Sim | Sim | Sim — por batalhao |
| 4 | Throughput por zona | Sim | Sim | Sim — Spark vs INSERT |
| 5 | Tempo de resposta analitico | Sim (Trino) | Sim | Sim — mesmas 5 queries |
| 6 | Taxa de compressao | Sim | Sim | Sim — tamanho em disco |
| 7 | Schema evolution | Sim | Sim | Sim — automatico vs manual |

### 6.5 Ressalvas experimentais

1. **Ambiente controlado:** todos os indicadores sao coletados em maquina local com Docker Compose. Os resultados refletem o desempenho em ambiente laboratorial, nao em condicoes operacionais reais (posto de comando, rede satelital, carga concorrente).

2. **Dados sinteticos:** os dados nao sao operacionais reais do EB. A intermitencia de conectividade e simulada via atrasos programados no gerador, nao por condicoes reais de rede.

3. **Escala reduzida:** o volume de dados e compativel com uma PoC academica (milhares a dezenas de milhares de registros), nao com o volume de uma operacao real com dezenas de simuladores simultaneos.

4. **Valores de referencia:** os valores da dissertacao de DaNova (PPCA/UnB) servem como **ordem de grandeza**, nao como benchmark direto. DaNova operou em rede local sem simulacao de intermitencia, usou Dremio+Nessie (nao Trino+Hive Metastore), e avaliou um contexto diferente (simulacao de combate, nao C2 tatico).

---

## 7. Alinhamento Doutrinario

### 7.1 Conformidade com os manuais do EB

| Manual | Ponto de alinhamento |
|---|---|
| **EB70-MC-10.205** | O Lakehouse apoia, nao substitui, o C2 exercido pelo estado-maior. O COC e produto humano; o sistema fornece os dados |
| **EB70-MC-10.211 (PPCOT)** | O pipeline Bronze->Silver->Gold suporta os produtos do PPCOT. As visoes Gold sao mapeadas a produtos doutrinarios (COP, quadro de situacao). O Indicador 5 mede se o Lakehouse acelera a fase de analise do PPCOT |
| **EB70-MC-10.336 (PITCIC)** | A camada Silver pode automatizar parte da fusao dos fatores Terreno, Meteorologia, Inimigo e Consideracoes Civis quando os dados chegam estruturados |
| **MC 3.11-10** | A escolha do modelo batch de ingestao e fundamentada diretamente nas restricoes de comunicacoes taticas documentadas neste manual. O Indicador 3 (latencia ponta-a-ponta por batalhao) **quantifica** a degradacao que o MC 3.11-10 preve qualitativamente |

### 7.2 O que a arquitetura nao endereca (escopo explicito)

- **Seguranca da informacao e controle de acesso**
- **Sincronizacao edge-to-central** (a PoC simula dados sinteticos)
- **Fluxo descendente (brigada -> batalhao)**
- **Decisao autonoma** (o sistema informa; nao decide)
- **Afirmacoes sobre sistemas internos do EB** (o baseline e paradigmatico, nao representativo de um sistema especifico)

---

## 8. Contribuicao Academica

### 8.1 Lacuna identificada

A literatura de arquitetura de dados (Sawadogo e Darmont, 2021; Hai et al., 2023; Giebler et al., 2021a) raramente trata o contexto de **conectividade intermitente com dados taticos militares heterogeneos**. Kapidani et al. (2022) aborda vigilancia maritima com C2 e Guo et al. (2021) trata de reconhecimento de campo de batalha, mas nenhum modela o ciclo completo de ingestao batch com proveniencia explicita em ambiente de conectividade variavel. Nenhum dos trabalhos do portfolio utiliza Apache Iceberg como formato de tabela, e nenhum propoe uma comparacao empirica Lakehouse vs paradigma relacional no contexto militar.

### 8.2 Contribuicao da dissertacao

1. **Uma arquitetura de referencia** para sistemas de dados de C2 tatico com conectividade intermitente, usando tecnologias open-source e Apache Iceberg.
2. **Um modelo de proveniencia temporal** adaptado ao contexto C2: `timestamp_geracao`, `timestamp_chegada` e `snapshot_timestamp` como metricas de latencia decisoria.
3. **Uma comparacao empirica paradigmatica** Lakehouse (Iceberg + Trino) vs paradigma relacional (PostgreSQL), com 7 indicadores praticos e 5 queries de benchmark mapeadas a produtos doutrinarios do COC.
4. **Um alinhamento doutrinario explicito** entre decisoes arquiteturais e os manuais do EB — especialmente a fundamentacao do modelo batch no MC 3.11-10 e a quantificacao da degradacao de conectividade prevista na doutrina.
5. **Uma avaliacao de schema evolution e partition evolution** do Iceberg como resposta a volatilidade de requisitos em ambiente operacional tatico.

---

## 9. Roteiro de Implementacao da PoC

### Fase 1 — Infraestrutura (Semana 1-2)
- [ ] Docker Compose com MinIO, Spark, Trino, Airflow, PostgreSQL (Hive Metastore + baseline), OpenMetadata
- [ ] Configuracao do Hive Metastore com backend PostgreSQL para catalogo Iceberg
- [ ] Configuracao do Spark e Trino com conector Iceberg
- [ ] Teste de conectividade: Spark escreve tabela Iceberg, Trino le a mesma tabela

### Fase 2 — Gerador de Dados Sinteticos (Semana 2-3)
- [ ] Script Python com os tres perfis de batalhao
- [ ] Geracao de GPS, SITREP e metadados de sensor com timestamps duplos
- [ ] Exportacao em lotes JSON com atrasos simulados

### Fase 3 — Pipeline Bronze (Semana 3-4)
- [ ] DAG Airflow de ingestao Bronze
- [ ] Schema Bronze com metadados de proveniencia e hidden partitioning
- [ ] Teste: dados fora de ordem e com atraso

### Fase 4 — Pipeline Silver e Gold (Semana 4-5)
- [ ] Jobs Spark de normalizacao (Silver) e agregacao (Gold)
- [ ] Visoes Gold mapeadas aos produtos do COC
- [ ] Teste de schema evolution: novo campo no SITREP do Batalhao 2

### Fase 5 — Baseline e Benchmark (Semana 5-6)
- [ ] DAG de carga do PostgreSQL baseline com mesmos dados
- [ ] Tabelas e views PostgreSQL com engenharia equivalente
- [ ] 5 queries de benchmark definidas e testadas em ambos os ambientes
- [ ] Coleta dos 7 indicadores

### Fase 6 — Governanca e Documentacao (Semana 6-7)
- [ ] Catalogo OpenMetadata com tabelas registradas e linhagem
- [ ] DAG de manutencao Iceberg
- [ ] Documentacao dos resultados e analise comparativa

---

## 10. Perguntas em Aberto

1. **Volume de dados do experimento:** quantos registros por batalhao, por tipo, por cenario? A definicao impacta diretamente os indicadores de throughput e compressao.

2. **Duracao da simulacao:** o gerador simula quantas horas de operacao? Isso define o volume de dados e a quantidade de gaps de conectividade para o Batalhao 3.

3. **Catalogo Iceberg:** Hive Metastore (recomendado para simplicidade) ou REST Catalog? A decisao pode ser adiada para a Fase 1.

---

## 11. Referencias Bibliograficas Utilizadas

- Ait Errami et al. (2023) — Evolucao DW -> Data Lake -> Lakehouse em dados espaciais
- Basili et al. [204] — Metodologia Goal-Question-Metric (GQM)
- Bentaib et al. (2024) — Estruturas de armazenamento de DW a Lakehouse
- Bjurstrom et al. (2023) — Qualidade de dados e analistas em C2 com IA
- Cervantes e Kazman [124] — Vetores Arquiteturais (Architecture Drivers)
- DaNova (PPCA/UnB) — Arquitetura de integracao de dados de simulacao de combate do EB
- Databricks (2025) — Arquitetura Medallion
- Diouan et al. (2025) — Tipologia de relacionamentos em Data Lakes
- Eichler et al. (2020) — Modelo de metadados HANDLE
- Giebler et al. (2019b) — Modelagem de Data Lakes com Data Vault
- Giebler et al. (2020) — Modelo de referencia por zonas para Data Lakes
- Giebler et al. (2021a) — Data Lake Architecture Framework (DLAF)
- Guo et al. (2021) — Reconhecimento de situacao de campo de batalha com Data Lake
- Hai et al. (2023) — Survey sobre funcoes e sistemas de Data Lakes
- ISO/IEC 25010 [126] — Norma de atributos de qualidade de software
- Kapidani et al. (2022) — Data Lake multicamadas em vigilancia maritima com C2
- Langleite et al. (2023) — Processamento autonomo de informacoes no nivel tatico
- Nambiar e Mundra (2022) — Comparacao Data Warehouse vs Data Lake
- Peerdeman et al. (2024) — C2 em Operacoes Multidominio (MDO)
- Putnam et al. (2020) — Proveniencia de dados em ambiente de C2
- Quix, Hai e Vatov (2016) — Sistema de metadados GEMMS
- Reis e Housley [120] — Aspectos transversais de engenharia de dados
- Richards e Ford [122, 125] — Arquitetura de software e atributos de qualidade
- Sawadogo e Darmont (2021) — Survey sobre arquiteturas e metadados em Data Lakes
- Sawadogo et al. (2019) — Modelo MEDAL para metadados em Data Lakes
- Scholly et al. (2021) — Modelo goldMEDAL para metadados genericos
- Verhoosel et al. (2020) — C2 orientado a dados e a servicos
- Zaharia et al. (2021) — Plataforma Lakehouse
- Zhao et al. (2021) — Arquitetura de Data Lake por zonas para IoT/Big Data
- Zhou et al. (2017) — Framework semantico de integracao para C2 agil

**Documentos doutrinarios do EB:**
- EB70-MC-10.205 — Comando e Controle
- EB70-MC-10.211 — PPCOT
- EB70-MC-10.336 — PITCIC
- MC 3.11-10 — As Comunicacoes na Brigada
