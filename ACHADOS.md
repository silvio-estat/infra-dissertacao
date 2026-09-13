# Achados

Coisas que custaram tempo para descobrir e que não estão óbvias no código.
Lista corrida, do mais novo para o mais antigo. Serve de lembrete — e boa parte
vira texto na dissertação.

**Como ler os números de acerto deste arquivo.** Compilar informação de um lugar
para outro erra também quando é feito por gente. A arquitetura não tira do
militar a responsabilidade pela interpretação: ela **reduz o tempo de cruzar as
informações** e deixa a origem de cada valor rastreável para quem confere. Um
acerto de 95% não é "a IA erra 5%"; é "o cruzamento que levaria horas sai em
minutos, e os 5% são localizáveis".

## PDF escaneado: o texto sai, a tabela não — até trocar de ferramenta (13/set/2026)

- **OCR de texto corrido embaralha tabela.** O tesseract lê os valores, mas não
  diz a que coluna pertencem. No Relatório de Bombardeio, codinome, hora e
  coordenada saem soltos e fora de ordem.
- **Um modelo de linguagem não conserta isso.** O `qwen3.5:4b` sobre o texto do
  OCR estruturou bem a prosa do INTEL e mal a tabela do RELPER: o combustível
  (56,1) foi parar em "baixas". Sem a posição, não há o que inferir.
- **Um leitor de layout conserta.** O Docling 2.126 (TableFormer + RapidOCR, em
  CPU, fora da stack) reconstrói a grade:

  | | tesseract | Docling |
  |---|---|---|
  | RELPER — números da planilha lidos (7 pares) | 79,3% | **100%** (276/276) |
  | RELPER — número na **célula certa** | não dá a coluna | **95,3%** (263/276) |
  | FOGOS — coordenada certa na coluna "Área Bombardeada" | colunas embaralhadas | **11/11** |
  | tempo por PDF, CPU | 0,7 s | ~5 s (o 1º: 73 s, carregando modelos) |

  12 dos 13 erros do RELPER são **uma linha inteira** que ele não achou. No
  FOGOS, a troca temida — a posição do observador no lugar da área
  bombardeada — não aconteceu nenhuma vez.
- **O limite muda de lugar.** "Tabela escaneada não dá" era verdade do OCR de
  texto, não do dado. Ressalvas: 7 tabelas e 11 bombardeios; PDFs sintéticos com
  degradação inventada por nós; dígito lido errado continua sendo número válido
  e passaria em silêncio.

## INTEL: a cadeia OCR → modelo de linguagem (13/set/2026)

- **Amostra antes de construir.** Sobre os 30 informes: número e data-hora 30/30,
  lugar 29/30, tipo 14/14 e prioridade 8/14 contra o gabarito. Custou 2 minutos
  e evitou construir sobre um lugar que falhava.
- **A lista de lugares atrapalhava.** Com os 70 nomes do gazetteer no pedido, o
  modelo devolveu vazio em 5 informes de nome perfeitamente legível. Pedindo o
  nome "como está escrito" — e deixando a consulta à `REF_GAZETTEER` resolver —
  foi a 29/30, e o tipo subiu de 12 para 14/14. Pedido menor, resposta melhor.
- **O OCR lê "A2" como "AZ"** e o modelo copia. `split_escala` passou a validar
  cada posição contra o domínio: fora da escala vira vazio, não sujeira.
- O pedido montado pela DAG tem **o mesmo hash** do pedido medido na amostra: o
  número de acerto vale para o que roda, não para uma versão parecida.

## Gerador: dois deslizes de fuso horário (13/set/2026)

Nenhum dos dois está em `defeitos.csv`: não são defeitos plantados.

- `gerar_intel.py` escreve a hora do fato, no corpo do informe, **em UTC** (e o
  mês em inglês, "DEC"), enquanto o cabeçalho usa -03. A receita do INTEL lê a
  hora como UTC (`fuso: UTC`) — se o gerador for corrigido, a receita muda.
- `gerar_fogos.py` escolhe **o dia do relatório pela data UTC**: um bombardeio
  às 22h24 de 25/11 em Brasília está no relatório de 26/11.
- Causa comum: `local(...).tzinfo` parece o fuso local, mas `local()` devolve a
  data já convertida para UTC. Ambos só apareceram porque o pareamento com o
  gabarito casou em UTC e não em -03.

## Medir errado: quando o erro era da régua (13/set/2026)

Quatro vezes neste dia um número ruim era defeito da **conferência**, não da
ferramenta. Conferir o medidor antes de acreditar no número.

- **Relato × fato pelos 40 primeiros caracteres** reutilizava o mesmo relato para
  fatos de mesma descrição e perdia os abreviados. Os acertos das versões de
  prompt (v1 65/29, v2 100/35, v3 100/59) estavam **errados**; com pareamento
  por hora + descrição + local, um a um: 45/45, 73/45, 91/64
  (`scripts/medir_acerto_relato.py`, que lê as versões apagadas pelo snapshot
  do Iceberg).
- **Coluna pelo texto do cabeçalho** dava 40,6% ao Docling; o OCR lê "Et Prev".
  Por posição, 95,3%.
- **Saída do `trino` no terminal estraga "ç"**: "Nucleo Gonçalo-Alves" parecia
  não existir no gazetteer. Contar dentro do SQL, ou passar o nome em
  hexadecimal (`from_utf8(from_hex(...))`).
- **Comparar instantes até o segundo** quando o documento só escreve hora e
  minuto dá 0/14 para 14 horas certas.

## Falhas silenciosas do pipeline (13/set/2026)

- **Erro de consulta lido como "nada pendente".** Depois de um reinício, com o
  Trino ainda subindo, a tarefa `relato` marcou sucesso sem fazer nada: a função
  que conta pendências engolia qualquer exceção. Agora só "tabela inexistente"
  conta como vazio; o resto deixa a tarefa vermelha.
- **Gravar uma vez só, no fim, perde tudo.** O mesmo reinício derrubou a tarefa no
  relato 250 de 300, e nada tinha sido gravado. Toda tarefa de extração grava a
  cada 50 e recomeça de onde parou.
- **A conta de pendências contava a própria saída.** Transcrição pendente era
  "extração de áudio sem filha" — e as 150 interpretações do modelo também são
  extrações de áudio sem filha. Rodar a DAG gravaria o modelo interpretando a si
  mesmo.
- **Evento sem chave.** Os 150 eventos de voz tinham `EVENTO_IDT` nulo, porque a
  receita não dizia o que identifica uma mensagem de rádio. O `MERGE` casa pela
  chave; nulo nunca casa — cada rodada duplicaria a voz. Não aparecia porque a
  voz não estava na DAG da Silver.
- **Evento criado antes da interpretação nunca era atualizado.** A DAG da Silver
  só procurava "registro sem evento"; os 300 relatos viraram evento antes do
  modelo e ficaram sem tipo. Agora conta também "evento sem interpretação".

## Prompt: versão, conteúdo e deriva (13/set/2026)

- **O rótulo não fixa o texto.** Os prompts são montados do modelo canônico e do
  gazetteer a cada execução: mudar um `quando:` muda o prompt sem mudar
  `relato-v3`. A tabela `AJUSTE_EXTRACAO` guarda texto, parâmetros e hash de
  cada versão; antes de extrair, a tarefa recalcula o hash e trava se o mesmo
  código vier com outro conteúdo.
- **Raciocínio para a prioridade não pagou.** A `relato-v5` (segunda chamada, com
  raciocínio) foi de 64% para 77% na prioridade — dentro do erro de 22 fatos —
  a 11 s por relato contra 0,9 s. Ficou a v3.
- **Exemplo do prompt contaminava a medida.** "Viatura da fração atolada na via"
  era a descrição de 4 fatos do gabarito; a medida honesta exclui esses 4.

## Docker Desktop: a cópia do arquivo ficou presa (13/set/2026)

- Editar um arquivo montado no contêiner **trocando-o por outro** (novo inode,
  como fazem editores e ferramentas de escrita atômica) deixou os contêineres
  vendo a versão antiga do `modelo_canonico.yaml` — o `--ddl` rodou sobre o YAML
  velho sem avisar. Regravar o conteúdo **no mesmo arquivo** destravou. Conferir
  o tamanho do arquivo dentro do contêiner antes de rodar.

## Governança: a mensagem de erro mentia (12/set/2026)

- **`SourceConnectionException: CheckAccess` não era problema de conexão.** Era
  conflito de dependência Python: instalar as bibliotecas de extração
  (`openpyxl`, `pytesseract`, `pdf2image`, `faster-whisper`) subiu o `click` para
  8.5, e o `collate-sqlfluff` — dependência do `openmetadata-ingestion` — exige
  `click<8.4.0`. O `CheckAccess` quebrava ao **carregar as bibliotecas**, antes
  de tentar conectar. A imagem do Airflow agora fixa `click<8.4.0` por último.
- **A causa estava na linha 17 do log, não nas últimas.** Procurar o erro pelo
  fim custou: testei Trino, metastore, credenciais e reiniciei a stack inteira à
  toa. Numa falha de ferramenta, ler o log do início é mais barato.
- Sintoma associado: os 7 testes de qualidade voltam `Aborted` — mesmo motivo.
- **Defeito separado, também corrigido:** a conexão do serviço `trino_lakehouse`
  gravada no OM estava sem `connectionArguments: {http_scheme: http}`. Sem isso
  o cliente tenta HTTPS num Trino que fala HTTP puro. Não era a causa acima, mas
  quebraria qualquer ingestão disparada pela tela do OpenMetadata.

## Modelo de linguagem sobre a transcrição (12/set/2026)

- **O classificador recupera o que a transcrição perde — e perde o que ela acerta.**
  Sobre os mesmos 150 áudios:

  | | comparação literal | `qwen3.5:4b` |
  |---|---|---|
  | codinome | 93% | 87% |
  | topônimo | 71% | **87%** |

  Onde a transcrição erra o nome (`posto mangavo`), o modelo reconhece e
  normaliza; a comparação literal não tinha chance. Mas onde a transcrição
  acertou, o modelo às vezes escolhe outro termo da lista. Combinar os dois —
  literal primeiro, modelo como reserva — daria ~95% e 87%.
- **3 de 150 lugares foram inventados**, apesar de a instrução exigir cópia
  literal da lista. Viram nulo na consulta: falham em silêncio, não em erro.
- **GPU vale 22×, mas só depois de aquecida.** 0,8 s por mensagem na RTX 5060 Ti
  contra 18,2 s em CPU — e a primeira chamada leva 58 s carregando o modelo. Sem
  descartar esse aquecimento a média dava 12,3 s e o ganho parecia 1,5×.
- **Docker Desktop no Linux não expõe a GPU ao contêiner** (o daemon roda numa
  VM). O Ollama escapou por ser um *servidor*: saiu do Docker e fala por HTTP.
  Uma biblioteca no lugar dele — Docling, por exemplo — ficaria presa em CPU.
- Raciocínio (`think`) **ligado piora**: mesmo tempo e resposta vazia, porque o
  modelo gasta o orçamento pensando num campo separado e não sobra JSON.

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
