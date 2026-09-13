# Prompts dos modelos de IA

Registro de todo texto enviado a um modelo de IA na extração da Bronze (DAG `2_bronze_extracao`).
Cada seção corresponde a um valor gravado em `EXTRACAO.PROMPT_VERSAO_COD`.

**O texto abaixo é o prompt MONTADO — o que o modelo de fato recebeu —, não o código.** Os prompts
são gerados a partir do modelo canônico (`documento:`, `tipos_possiveis:`, o `quando:` e os
`sinonimos:` dos domínios) e de `seeds/gazetteer.csv`. Ao texto de cada versão a DAG acrescenta a
entrada: `\nRELATO: <texto>\n` ou `\nTRANSCRICAO: <texto>\n`.

Todas as chamadas ao `qwen3.5:4b` usam Ollama no host, `temperature: 0`. Salvo indicação, com
`format: json` e `think: false`.

## Resumo

| Versão | Modelo | Lê | Tempo por item | Resultado | Onde está a saída |
|---|---|---|---|---|---|
| `vocab-v1` | faster-whisper `medium` int8 | `.wav` | 5,6 s | codinome 93% · topônimo 71% · WER 21,6% | tabela |
| `voz-v1` | qwen3.5:4b | transcrição | 0,8 s | codinome 87% · topônimo 87% | tabela |
| `relato-v1` | qwen3.5:4b | relato do C2_B | 0,9 s | tipo 45% · prioridade 45% | snapshot `5302099064065083570` |
| `relato-v2` | qwen3.5:4b | relato do C2_B | 0,8 s | tipo 73% · prioridade 45% | snapshot `218242516275008809` |
| **`relato-v3`** | qwen3.5:4b | relato do C2_B | 0,9 s | **tipo 91% · prioridade 64%** | snapshot `1291687178912460115` |
| `relato-v4` | qwen3.5:4b, 2 chamadas | relato do C2_B | 1,0 s | 300 respostas vazias | snapshot `843852446770918492` |
| `relato-v5` | qwen3.5:4b, 2 chamadas | relato do C2_B | 11,1 s | tipo 91% · prioridade 77% | tabela |

**Em uso na DAG: `vocab-v1`, `voz-v1` e `relato-v3`.** A v3 foi escolhida em 13/09 pela rapidez: a v5
ganha 3 fatos de 22 na prioridade (intervalos de confiança sobrepostos) por ~12 vezes o tempo, e o
objetivo do estudo é mostrar que o texto livre cruza com as outras fontes, não calibrar o prompt.

**Mas as 300 respostas que estão em `EXTRACAO` hoje são `relato-v5`** — já tinham rodado quando a v3
foi escolhida, e não se reprocessou o que já estava pronto. A DAG só produz `relato-v3` numa rodada
nova, do zero. Cada linha diz a própria versão em `PROMPT_VERSAO_COD`, então a linhagem não mente:
a v3 é o que a DAG faria, a v5 é o que ela fez.

Acerto do relato: 22 fatos do gabarito (`dados_sinteticos/verdade/fatos.csv`), medido por
`scripts/medir_acerto_relato.py`, que também dá o intervalo de Wilson e lê as versões apagadas
pelo snapshot. Nos 278 relatos de rotina, a prioridade saiu ROTINA em todos, em todas as versões.
**Contaminação a partir da v3:** o exemplo `'Viatura da fracao atolada na via...'` é a descrição de
4 fatos do gabarito. Sem eles (n = 18): v3 tipo 89% · prioridade 61%; v5 89% · 72%.

Acerto da voz: 150 áudios contra `dados_sinteticos/verdade/gabarito.csv` (ver `ACHADOS.md`).

---

## `vocab-v1` — vocabulário do transcritor

Não é pedido a um modelo de linguagem: é o `initial_prompt` do faster-whisper, um texto que o
transcritor recebe como se fosse "o que já foi dito antes", para saber que palavras raras existem.
Os codinomes são palavras comuns com sentido incomum e os topônimos são inventados.

No modelo `small`, o vocabulário não mudou nada (testados lista longa, só codinomes e `hotwords`).
O ganho de 65% para 93% no codinome veio da troca de `small` para `medium`, com este vocabulário.

```
Rede de radio militar. Termos: BR-154 km 0, BR-154 km 10, BR-154 km 100, BR-154 km 105, BR-154 km 110, BR-154 km 115, BR-154 km 120, BR-154 km 15, BR-154 km 20, BR-154 km 25, BR-154 km 30, BR-154 km 35, BR-154 km 40, BR-154 km 45, BR-154 km 5, BR-154 km 50, BR-154 km 55, BR-154 km 60, BR-154 km 65, BR-154 km 70, BR-154 km 75, BR-154 km 80, BR-154 km 85, BR-154 km 90, BR-154 km 95, Curva do Baru, Entroncamento BR-154 / VC-231, Fazenda Pau-Terra, Fazenda Sucupira, Morro da Barriguda, Morro do Jatoba, Nucleo Gonçalo-Alves, Nucleo Jatoba, Nucleo Mangaba, Nucleo Pequi, Ponte sobre o Corrego Aroeira, Ponte sobre o Corrego Pequi, Posto Mangaba, Povoado Barriguda, Povoado Cagaita, Povoado Pau-Terra, Povoado Sucupira, Quadricula 4471, Quadricula 4472, Quadricula 4473, Quadricula 4474, Quadricula 4571, Quadricula 4572, Quadricula 4573, Quadricula 4574, Quadricula 4671, Quadricula 4672, Quadricula 4673, Quadricula 4674, Represa da Copaiba, Serra da Cagaita, VC-231 km 0, VC-231 km 10, VC-231 km 15, VC-231 km 20, VC-231 km 25, VC-231 km 30, VC-231 km 35, VC-231 km 40, VC-231 km 5, Vau do Corrego Macauba, Vila Aroeira, Vila Baru, Vila Copaiba, Vila Macauba, barreira, bigorna, brasa, campo de minas, cavalo, celeiro, cratera, eixo de progressao, fosso anticarro, limite, linha de fase, linha de partida, martelo, objetivo, ponto de controle, potro, tambor, tordilho, zona de reuniao.
```

---

## `voz-v1` — o modelo de linguagem lê a transcrição

A tarefa é **normalizar**, não interpretar: reconhecer, apesar do erro de transcrição, qual termo
das listas foi dito, e devolvê-lo escrito como na lista — porque o que vem depois é consulta exata
(o domínio casa por sinônimo, o gazetteer casa por nome). Um valor inventado não casa e some em
silêncio; por isso a regra de devolver `null`.

Antes desta versão houve rascunhos usados só para medir tempo e o efeito do raciocínio (ligado, o
modelo devolvia resposta vazia). Não gravaram nada em `EXTRACAO`.

```
Voce le a transcricao de uma mensagem de radio militar. A transcricao TEM ERROS: nomes proprios saem trocados por palavras parecidas. Reconheca, apesar do erro, qual termo das listas abaixo foi dito.

CODINOMES: barreira, bigorna, brasa, campo de minas, cavalo, celeiro, cratera, eixo de progressao, fosso anticarro, limite, linha de fase, linha de partida, martelo, objetivo, ponto de controle, potro, tambor, tordilho, zona de reuniao

LUGARES: BR-154 km 0, BR-154 km 10, BR-154 km 100, BR-154 km 105, BR-154 km 110, BR-154 km 115, BR-154 km 120, BR-154 km 15, BR-154 km 20, BR-154 km 25, BR-154 km 30, BR-154 km 35, BR-154 km 40, BR-154 km 45, BR-154 km 5, BR-154 km 50, BR-154 km 55, BR-154 km 60, BR-154 km 65, BR-154 km 70, BR-154 km 75, BR-154 km 80, BR-154 km 85, BR-154 km 90, BR-154 km 95, Curva do Baru, Entroncamento BR-154 / VC-231, Fazenda Pau-Terra, Fazenda Sucupira, Morro da Barriguda, Morro do Jatoba, Nucleo Gonçalo-Alves, Nucleo Jatoba, Nucleo Mangaba, Nucleo Pequi, Ponte sobre o Corrego Aroeira, Ponte sobre o Corrego Pequi, Posto Mangaba, Povoado Barriguda, Povoado Cagaita, Povoado Pau-Terra, Povoado Sucupira, Quadricula 4471, Quadricula 4472, Quadricula 4473, Quadricula 4474, Quadricula 4571, Quadricula 4572, Quadricula 4573, Quadricula 4574, Quadricula 4671, Quadricula 4672, Quadricula 4673, Quadricula 4674, Represa da Copaiba, Serra da Cagaita, VC-231 km 0, VC-231 km 10, VC-231 km 15, VC-231 km 20, VC-231 km 25, VC-231 km 30, VC-231 km 35, VC-231 km 40, VC-231 km 5, Vau do Corrego Macauba, Vila Aroeira, Vila Baru, Vila Copaiba, Vila Macauba

Devolva so um JSON com tres chaves:
  "codinome": um termo COPIADO da lista CODINOMES, ou null se nenhum foi dito
  "referencia_local": um nome COPIADO da lista LUGARES, ou null
  "texto": a mensagem sem o indicativo da estacao e sem as palavras de protocolo

Regra rigida: codinome e referencia_local so podem conter texto que exista LITERALMENTE nas listas. Se o que foi dito nao estiver na lista, devolva null. Nunca escreva coordenadas, quadriculas ou nomes proprios que nao estejam listados.
```

---

## `relato-v1` — lista de códigos, sem critério

Montado a partir dos campos que a receita marca `por: llm`, com os nove tipos do domínio e seus
rótulos.

**Resultado:** tipo 45% · prioridade 45%. `SITUACAO_UNIDADE` (tipo do formulário periódico, que um
relato nunca é) em 143 dos 300 relatos: sem critério, o modelo escolhe o valor cujo NOME lembra o
assunto. A regra "use o valor mais conservador" fabricou ROTINA em massa, e as escalas de
confiabilidade e credibilidade foram preenchidas em todos os 300 relatos, por invenção.

```
Voce le o relato de um observador militar, escrito a mao no campo, e o classifica.
Responda so um JSON com estas chaves, usando SEMPRE um dos valores listados:
  "TIPO_COD": um destes — POSICAO (Relato de posicao), AVISTAMENTO (Avistamento), AVISTAMENTO_INIMIGO (Avistamento de forca oponente), INCIDENTE (Incidente), OBSTACULO (Obstaculo), MCC (Medida de coordenacao e controle), MISSAO_TIRO (Missao de tiro), BOMBARDEIO_INIMIGO (Bombardeio inimigo observado), SITUACAO_UNIDADE (Situacao periodica da unidade)
  "PRIORIDADE_COD": um destes — URGENTE (Urgente), PRIORITARIO (Prioritario), ROTINA (Rotina)
  "FONTE_CONFIABILIDADE_COD": um destes — A (Fonte confiavel), B (Fonte geralmente confiavel), C (Fonte razoavelmente confiavel), D (Fonte nao usualmente confiavel), E (Fonte nao confiavel), F (Confiabilidade nao pode ser julgada)
  "INFO_CREDIBILIDADE_COD": um destes — 1 (Confirmada por outras fontes), 2 (Provavelmente verdadeira), 3 (Possivelmente verdadeira), 4 (Duvidosa), 5 (Improvavel), 6 (Veracidade nao pode ser julgada)

Regra rigida: nunca invente valor fora das listas. Se o relato nao permitir decidir, use o valor mais conservador da lista.
```

---

## `relato-v2` — o que o documento é e quando cada valor se aplica

Três acréscimos, todos declarados no modelo canônico, não na DAG: o `documento:` da receita (o que
o modelo está lendo), os `tipos_possiveis:` da fonte (4 em vez de 9) e o `quando:` de cada valor.
A regra do "mais conservador" virou "devolva null".

**Resultado:** tipo 73% · prioridade 45%. `ROTINA` — valor de prioridade — escrito dentro de
`TIPO_COD` em 30 relatos: as duas listas soltas se confundiram. Confiabilidade e credibilidade
nulas em 300 de 300, como instruído.

```
Voce le RELATOS DE OBSERVADOR. Texto curto digitado no campo por um militar, dentro do aplicativo de C2, descrevendo o que ele esta vendo ou fazendo naquele momento. Nao usa codigos: e linguagem comum, as vezes abreviada. Quem relatou, quando e de onde ja estao em outros campos do registro — o texto traz APENAS a situacao.

Um relato descreve uma destas coisas, e so estas:
  POSICAO                a fracao informa onde esta e o que faz; nada de novo aconteceu
  AVISTAMENTO            viu algo digno de nota que NAO e forca oponente: pessoas, veiculos civis, movimento de populacao
  AVISTAMENTO_INIMIGO    viu forca oponente: tropa, viatura militar ou indicio de presenca inimiga
  INCIDENTE              algo deu errado ou exige providencia: acidente, avaria, obstaculo na via, pessoa ferida

Prioridade, pelo que o texto exige de quem recebe:
  URGENTE                risco imediato a tropa ou a missao; exige acao agora
  PRIORITARIO            precisa de providencia, mas nao e imediato
  ROTINA                 informativo; nada a fazer

Responda so um JSON com as chaves: "TIPO_COD", "PRIORIDADE_COD", "FONTE_CONFIABILIDADE_COD", "INFO_CREDIBILIDADE_COD"

FONTE_CONFIABILIDADE_COD e INFO_CREDIBILIDADE_COD sao juizo de analista sobre QUEM relatou e sobre a informacao; o texto quase nunca permite decidir. Devolva null nas duas, salvo se o proprio texto trouxer a avaliacao.
Se o texto nao permitir decidir um campo, devolva null. Nunca invente valor fora dos listados.
```

---

## `relato-v3` — cada lista amarrada à sua chave, e três exemplos (EM USO)

Cada lista aparece numerada junto da chave que ela preenche, e a prioridade ganha três exemplos
concretos: para julgamento, exemplo move o resultado; a definição abstrata da v2 não tinha movido.

**Resultado:** tipo 91% · prioridade 64%. `ROTINA` no tipo caiu de 30 para 2.

```
Voce le RELATOS DE OBSERVADOR. Texto curto digitado no campo por um militar, dentro do aplicativo de C2, descrevendo o que ele esta vendo ou fazendo naquele momento. Nao usa codigos: e linguagem comum, as vezes abreviada. Quem relatou, quando e de onde ja estao em outros campos do registro — o texto traz APENAS a situacao.

Responda so um JSON com exatamente estas quatro chaves:

1) "TIPO_COD" — o que o relato descreve. Use SO um destes:
  POSICAO                a fracao informa onde esta e o que faz; nada de novo aconteceu
  AVISTAMENTO            viu algo digno de nota que NAO e forca oponente: pessoas, veiculos civis, movimento de populacao
  AVISTAMENTO_INIMIGO    viu forca oponente: tropa, viatura militar ou indicio de presenca inimiga
  INCIDENTE              algo deu errado ou exige providencia: acidente, avaria, obstaculo na via, pessoa ferida

2) "PRIORIDADE_COD" — o que o relato exige de quem o recebe. Use SO um destes:
  URGENTE                risco imediato a tropa ou a missao; exige acao agora
  PRIORITARIO            precisa de providencia, mas nao e imediato
  ROTINA                 informativo; nada a fazer
   Decida pela ACAO, nao pelo assunto. Exemplos:
     'Fracao instalada em BR-154 km 15, PC operando normalmente.'  -> ROTINA
     'Viatura da fracao atolada na via, solicito apoio.'           -> PRIORITARIO
     'Tropa inimiga a 500 m da posicao, em aproximacao.'           -> URGENTE
   Um relato que so informa e ROTINA. Um que pede providencia e PRIORITARIO.
   Um que indica risco agora e URGENTE.

3) "FONTE_CONFIABILIDADE_COD" e 4) "INFO_CREDIBILIDADE_COD" — juizo de
   analista sobre QUEM relatou e sobre a informacao. O texto de um relato
   quase nunca permite decidir: devolva null nas duas, salvo se o proprio
   texto trouxer a avaliacao.

Nunca use um valor de uma lista na chave da outra. Se o texto nao permitir
decidir um campo, devolva null. Nunca invente valor fora dos listados.
```

---

## `relato-v4` e `relato-v5` — segunda chamada, só para a prioridade, com raciocínio

O `think` do Ollama vale para a chamada inteira. Para raciocinar só sobre a prioridade, o relato
passa a ser lido duas vezes: a primeira com o prompt da **v3** (acima), a segunda com o pedido
abaixo, raciocínio ligado. A prioridade da segunda substitui a da primeira. O raciocínio volta num
campo separado (`thinking`) e é descartado.

- **v4:** a segunda chamada com `think: true` **e** `format: json`. Resultado: 300 respostas vazias.
  O modelo gasta o orçamento no raciocínio e não sobra saída.
- **v5:** a segunda chamada com `think: true` e **sem** `format`; o JSON é recortado do texto da
  resposta. Resultado: tipo 91% · prioridade 77%, a 11,1 s por relato.

```
Voce le um RELATO DE OBSERVADOR. Texto curto digitado no campo por um militar, dentro do aplicativo de C2, descrevendo o que ele esta vendo ou fazendo naquele momento. Nao usa codigos: e linguagem comum, as vezes abreviada. Quem relatou, quando e de onde ja estao em outros campos do registro — o texto traz APENAS a situacao.

Decida o que este relato exige de quem o recebe — pela ACAO necessaria,
nao pelo assunto:
  URGENTE        risco imediato a tropa ou a missao; exige acao agora
  PRIORITARIO    precisa de providencia, mas nao e imediato
  ROTINA         informativo; nada a fazer

  'Fracao instalada em BR-154 km 15, PC operando normalmente.'  -> ROTINA
  'Viatura da fracao atolada na via, solicito apoio.'           -> PRIORITARIO
  'Tropa inimiga a 500 m da posicao, em aproximacao.'           -> URGENTE

Responda so um JSON: {"PRIORIDADE_COD": <um dos tres>}
```

---

## Este arquivo e a tabela `AJUSTE_EXTRACAO`

Este arquivo conta a HISTÓRIA das versões (o que mudou e por quê). O CONTEÚDO de cada código —
texto montado, parâmetros e hash — está na tabela `bronze.AJUSTE_EXTRACAO`, que é a fonte de
verdade consultável: `EXTRACAO.PROMPT_VERSAO_COD` liga cada extração à sua linha lá. As 8 versões
acima (incluindo `psm4-200dpi`, do OCR, que não tem texto) foram carregadas em 13/09/2026; as
seguintes são registradas pela própria DAG no primeiro uso.

O rótulo da versão é escrito à mão na DAG, mas o texto é montado do YAML e do gazetteer a cada
execução: mudar o `quando:` de um valor muda o prompt sem mudar o rótulo. Por isso, antes de
extrair, cada tarefa recalcula o hash e o compara com o da tabela — mesmo código com hash
diferente PARA a tarefa, e a saída é criar uma versão nova. Para `relato-v1`, `v2`, `v4` e `v5`
o texto na tabela foi copiado deste arquivo; nas quatro versões em uso ele foi montado pela DAG.
