# Achados

Coisas que custaram tempo para descobrir e que não estão óbvias no código.
Lista corrida, do mais novo para o mais antigo. Serve de lembrete — e boa parte
vira texto na dissertação.

## Transcrição de voz (12/set/2026)

- **`medium` compensa, e o ganho está onde importa.** Sobre os mesmos 150 áudios:

  | | `small` | `medium` |
  |---|---|---|
  | tempo por áudio | 2,2 s | 5,6 s |
  | WER global | 29,4% | 21,6% |
  | **codinome reconhecido** | 65% | **93%** |
  | topônimo reconhecido | 71% | 71% |

  O codinome vira `TIPO_COD`: passou de um evento sem tipo a cada três para um a
  cada catorze. O topônimo não mudou — são nomes inventados, que nenhum modelo
  conhece; quem os resolve é o gazetteer, não o transcritor.
- Só 1 de 150 transcrições sai **exata**, nos dois modelos.
- **O WER engana.** Ele é dominado pelo indicativo (`aqui Tigre 4` → `Aqtive 4`)
  e pelas palavras de protocolo (`Orvalho`, `Câmbio`) — nenhuma delas usada pelo
  pipeline. O que o de/para consome sai melhor: **codinome 65%**, **topônimo
  71%**. Ao medir transcrição, medir o que será consumido, não a frase inteira.
- **Vocabulário inicial não resolveu.** Testados lista longa (codinomes +
  70 topônimos, 1.387 caracteres), só codinomes, e o parâmetro `hotwords` do
  faster-whisper 1.1: nenhum moveu o ponteiro. Corrige a impressão anterior
  (09/set), que veio de um teste com poucas mensagens.
- O modelo é baixado na **construção da imagem**, não na primeira execução: a
  DAG não depende de rede e o resultado não muda por troca de versão.

## OCR (12/set/2026)

- **Resolução maior piora.** Rasterizar o PDF a 300 dpi derrubou o acerto de 79%
  para 27%. O gerador digitalizou a 200 dpi; ampliar não cria informação, borra
  a que existe.
- **Modo de segmentação importa mais que resolução.** `--psm 4` (uma coluna de
  texto com tamanhos variados) contra o padrão `--psm 3`: 79,3% contra 75,7%,
  e nunca pior em nenhum dos 7 arquivos testados.
- **A medida saiu de graça.** 7 remessas do RELPER chegam em planilha *e* em
  escaneado. A planilha é a verdade por construção, então dá para contar quantos
  números dela aparecem no texto do OCR — sem gabarito à parte. Foi assim que a
  configuração foi escolhida por evidência, não por palpite.
- `PROMPT_VERSAO_COD` não é só para prompt de IA: é **o ajuste com que a
  ferramenta foi chamada** (`psm4-200dpi`). Sem ele o resultado não se reproduz.

## Spark (12/set/2026)

- **Leitor vetorizado do Iceberg derruba o executor.** Com
  `spark.sql.iceberg.vectorization.enabled` ligado, reler uma tabela durante um
  `MERGE` que *atualiza* linhas mata o executor com código 134 — travamento
  nativo, sem exceção Java. Sintoma traiçoeiro: a **primeira** carga passa (só
  insere); a falha só aparece ao reprocessar.
- **O driver do `SparkSubmitOperator` roda dentro do Airflow**, que não tem o
  `spark-defaults.conf` da imagem do Spark. Tudo o que aquele arquivo ajusta
  precisa ser repetido na `conf` da DAG.
- **Subconsulta correlacionada** no Spark não aceita `LIMIT` (use `max()`) nem
  coluna do tipo mapa.
- **O contêiner Spark roda Python 3.10** — f-string aninhada com a mesma aspa
  passa no Python do host e quebra lá dentro.
- **Não inferir esquema** de um `DataFrame` cujas colunas podem vir só com nulo.

## Dados sintéticos (11–12/set/2026)

- **Regerar muda os hashes.** `xlsx`, `pdf` e `wav` não saem byte a byte iguais
  (data de criação embutida, síntese de voz); `json` e `jpg` saem. Ou seja,
  regerar obriga a reconstruir a Bronze.
- **Acrescentar um sorteio ao gerador muda o cenário inteiro**, porque desloca a
  sequência de números aleatórios. Foi de 68 para 50 defeitos plantados ao
  acrescentar o par planilha/escaneado. Não é bug: é propriedade de gerador com
  semente.
- **Sidecar de conteúdo idêntico colapsava duas recepções em uma.** A mesma
  remessa em dois formatos tem sidecar com o mesmo texto; como o identificador
  vinha do hash do conteúdo, um dos binários ficava sem registro. Corrigido
  incluindo o arquivo acompanhado na identidade da recepção.

## OpenMetadata (11/set/2026)

- **`columnValuesToBeInSet` só conta.** Sem `matchEnum: true`, ele verifica
  quantos valores *estão* na lista e passa se achou algum — não verifica que
  todos estão. Três dos sete testes nasceram vazios por causa disso, e só o
  teste que deveria reprovar revelou o problema.
- **O perfil não aparece pela API.** `/tables/{id}/tableProfile` volta vazio
  mesmo com o perfil visível na tela. Conferir pela UI ou pelo log da tarefa.
- **O que é criado só pela tela morre no reset.** Os testes de qualidade da fase
  anterior se perderam assim. Tudo passa a ser declarado no repositório e
  empurrado por API.
