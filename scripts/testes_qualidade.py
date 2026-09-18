#!/usr/bin/env python3
"""
testes_qualidade.py — declara no OpenMetadata os testes de qualidade do repositorio.

Doze testes, escolhidos para MOSTRAR O QUE A PLATAFORMA FAZ — nao para cobrir o
modelo canonico inteiro (que renderia 76 testes estruturais e nenhum leitor). Sao
dois grupos:

  1. SETE de conformidade (Bronze) — o dado que chegou esta certo? Nulo, dominio,
     unicidade do hash, integridade da linhagem. Seis passam; `arquivo_operacao_da_pasta`
     falha de proposito: o gerador remeteu uma planilha na pasta do ano errado.

  2. CINCO de completude (Bronze e Silver) — FALTA dado? Sao os que respondem
     "como voce sabe que uma informacao nao chegou ao EM": arquivo recusado sem
     reenvio, binario sem sidecar, sidecar sem binario, turno que nenhuma OM
     remeteu e subunidade que sumiu de dentro de um relatorio que chegou. `relper_turno_remetido` falha de proposito
     e devolve os 11 turnos que o gerador deixou de remeter.

Os dois que falham de proposito sao o ponto, nao um defeito: e o que se mostra
quando a pergunta e "e os dados errados, como voce tratou?".

Criar um teste por aqui e o mesmo que cria-lo pela tela do OpenMetadata: a tela e
apenas um cliente desta API. O teste aparece na aba Data Quality da tabela, com
botao de rodar e historico. A diferenca e que aqui ele fica VERSIONADO no
repositorio — quem repetir o experimento tem os mesmos testes, e um reset do
OpenMetadata nao os leva embora.

    python3 scripts/testes_qualidade.py            # cria (ou atualiza) os doze
    python3 scripts/testes_qualidade.py --listar   # so mostra o que existe hoje

Depois de criar, quem EXECUTA os testes e a DAG 5_governanca (tarefa
data_quality), ou o botao "Run Now" na tela.
"""
import argparse
import json
import os
import urllib.error
import urllib.request

OM_URL = os.environ.get("OPENMETADATA_URL", "http://localhost:8585")
SERVICO = "trino_lakehouse.iceberg"

# As modalidades sao a coluna que sustenta a tese de heterogeneidade.
MODALIDADES = ["JSON", "TEXTO", "PLANILHA", "IMAGEM", "AUDIO", "PDF"]

# -----------------------------------------------------------------------------
# OS DOZE TESTES
# -----------------------------------------------------------------------------
# `porque` vira a descricao na tela do OpenMetadata: quem abrir o catalogo le
# a justificativa junto com o resultado.

TESTES = [
    {
        "nome": "recepcao_operacao_preenchida",
        "rotulo": "RECEPCAO_BRUTA.OPERACAO_COD nunca e nulo",
        "porque": "A operacao e o elo entre as seis fontes: sem ela, uma posicao de GPS e um "
                  "relatorio de bombardeio nao se cruzam e nenhuma visao Gold tem recorte. "
                  "O modelo declara nulo: false.",
        "tabela": "bronze.recepcao_bruta",
        "coluna": "operacao_cod",
        "definicao": "columnValuesToBeNotNull",
    },
    {
        "nome": "recepcao_modalidade_do_dominio",
        "rotulo": "RECEPCAO_BRUTA.MODALIDADE_COD so aceita as seis modalidades",
        "porque": "E a coluna que permite contar quantas formas fisicas de dado uma consulta "
                  "atravessou — a tese de heterogeneidade. Valor fora da lista invalidaria a conta.",
        "tabela": "bronze.recepcao_bruta",
        "coluna": "modalidade_cod",
        "definicao": "columnValuesToBeInSet",
        "parametros": {"allowedValues": MODALIDADES, "matchEnum": True},
    },
    {
        "nome": "arquivo_hash_unico",
        "rotulo": "ARQUIVO.CONTEUDO_HASH_COD e unico",
        "porque": "A Bronze deduplica pelo conteudo: o mesmo arquivo reenviado com outro nome nao "
                  "pode virar linha nova. O gerador planta tres fotos reenviadas justamente para "
                  "exercitar isso.",
        "tabela": "bronze.arquivo",
        "coluna": "conteudo_hash_cod",
        "definicao": "columnValuesToBeUnique",
    },
    {
        "nome": "extracao_indicador_de_ia",
        "rotulo": "EXTRACAO.INFERENCIA_INDIC so aceita S ou N",
        "porque": "E a coluna que responde 'quantos numeros passaram por IA'. Substituiu uma "
                  "confianca numerica que nao era comparavel entre ferramentas; se aceitar outro "
                  "valor, a pergunta deixa de ter resposta.",
        "tabela": "bronze.extracao",
        "coluna": "inferencia_indic",
        "definicao": "columnValuesToBeInSet",
        "parametros": {"allowedValues": ["S", "N"], "matchEnum": True},
    },
    {
        "nome": "arquivo_operacao_da_pasta",
        "rotulo": "ARQUIVO.OPERACAO_COD so deveria ter PERSEU_2024",
        "porque": "ESTE FALHA DE PROPOSITO. O estudo e de uma operacao so, mas OPERACAO_COD em "
                  "ARQUIVO vem da PASTA em landing/ — um palpite, nao o valor de referencia (que "
                  "e o do sidecar). O gerador plantou uma planilha remetida em landing/perseu_2023/. "
                  "O teste acusa a linha: e assim que um arquivo na pasta errada aparece.",
        "tabela": "bronze.arquivo",
        "coluna": "operacao_cod",
        "definicao": "columnValuesToBeInSet",
        "parametros": {"allowedValues": ["PERSEU_2024"], "matchEnum": True},
    },
    {
        "nome": "extracao_aponta_arquivo_existente",
        "rotulo": "Toda EXTRACAO aponta para um ARQUIVO que existe",
        "porque": "A linhagem nao pode ter furo: se uma extracao apontasse para um arquivo "
                  "inexistente, a pergunta 'de onde veio este numero' morreria no meio do caminho. "
                  "A consulta devolve as linhas orfas; zero linhas e o esperado. Nao apontar para "
                  "arquivo NENHUM (ARQUIVO_IDT nulo) e legitimo e fica de fora: e o caso do relato "
                  "do C2_B, texto que chega dentro do proprio JSON e nunca teve binario — sao 300 "
                  "extracoes assim, que o teste acusava como orfas ate 18/09.",
        "tabela": "bronze.extracao",
        "definicao": "tableCustomSQLQuery",
        "sql": "SELECT e.extracao_idt FROM iceberg.bronze.extracao e "
               "LEFT JOIN iceberg.bronze.arquivo a ON a.arquivo_idt = e.arquivo_idt "
               "WHERE e.arquivo_idt IS NOT NULL AND a.arquivo_idt IS NULL",
    },
    {
        "nome": "leitura_direta_nao_tem_modelo",
        "rotulo": "Leitura direta (INFERENCIA_INDIC = N) nao tem MODELO_NOME",
        "porque": "Regra do modelo: so ha modelo onde houve inferencia. Abrir uma planilha com "
                  "openpyxl e deterministico — registra-se a ferramenta e a versao, nunca um "
                  "modelo. A consulta devolve as linhas que violam a regra.",
        "tabela": "bronze.extracao",
        "definicao": "tableCustomSQLQuery",
        "sql": "SELECT extracao_idt FROM iceberg.bronze.extracao "
               "WHERE inferencia_indic = 'N' AND modelo_nome IS NOT NULL",
    },
    # -------------------------------------------------------------------------
    # OS QUATRO DE COMPLETUDE: nao perguntam se o dado que chegou esta certo,
    # e sim se FALTA dado — o que chegou pela metade e o que nao chegou.
    # -------------------------------------------------------------------------
    {
        "nome": "arquivo_tem_sidecar",
        "rotulo": "Todo ARQUIVO tem o sidecar que o acompanha",
        "porque": "Todo binario em landing/ chega em par com um .json de proveniencia. Se o "
                  "sidecar se perdeu na transmissao, o binario entra na Bronze sem operacao, sem "
                  "unidade remetente e sem hora — catalogado, mas mudo. A consulta devolve os "
                  "binarios orfaos; zero linhas e o esperado.",
        "tabela": "bronze.arquivo",
        "definicao": "tableCustomSQLQuery",
        "sql": "SELECT a.arquivo_uri_txt FROM iceberg.bronze.arquivo a "
               "LEFT JOIN iceberg.bronze.recepcao_bruta r ON r.arquivo_idt = a.arquivo_idt "
               "WHERE r.recepcao_idt IS NULL",
    },
    {
        "nome": "sidecar_achou_seu_binario",
        "rotulo": "Sidecar de fonte binaria aponta para um ARQUIVO",
        "porque": "O caso inverso do anterior: o .json chegou e o binario nao. A ingestao casa os "
                  "dois pelo nome sem a extensao; quando nao acha par, grava a recepcao com "
                  "ARQUIVO_IDT vazio — indistinguivel do JSON legitimo do C2_A, que nunca tem "
                  "binario. Por isso o teste olha so as fontes que SEMPRE trazem arquivo. A "
                  "consulta devolve os sidecares sem binario; zero linhas e o esperado.",
        "tabela": "bronze.recepcao_bruta",
        "definicao": "tableCustomSQLQuery",
        "sql": "SELECT origem_uri_txt FROM iceberg.bronze.recepcao_bruta "
               "WHERE sistema_origem_cod IN ('RELPER', 'FOGOS', 'INTEL', 'VOZ') "
               "AND arquivo_idt IS NULL",
    },
    {
        "nome": "relper_turno_remetido",
        "rotulo": "Toda OM remeteu o RELPER nos dois turnos de cada dia",
        "porque": "ESTE TESTE FALHA DE PROPOSITO. O EM da brigada fixa duas remessas por dia "
                  "(cadencia declarada em visoes/RELPER do modelo canonico), e REF_UNIDADE diz "
                  "quais OM existem: o cruzamento das duas coisas produz a lista do que DEVERIA "
                  "ter chegado, e o que sobra depois de subtrair o que chegou e a ausencia — que "
                  "e dado operacional, nao falha de TI (uma OM que nao reporta pode estar sem "
                  "comunicacao, deslocando ou em contato). O gerador deixou de remeter 11 turnos "
                  "de proposito; a consulta devolve exatamente esses 11, com OM, dia e turno. A "
                  "janela vai da primeira a ultima remessa observada, e nao do inicio ao fim da "
                  "operacao, para nao acusar como ausencia a borda em que nenhuma OM remeteu.",
        "tabela": "silver.situacao_unidade",
        "definicao": "tableCustomSQLQuery",
        "sql": """
WITH remessa AS (
  SELECT u.superior_cod AS om_cod, date(e.ocorrencia_data) AS dia, s.turno_cod
  FROM iceberg.silver.evento e
  JOIN iceberg.silver.situacao_unidade s ON s.evento_idt = e.evento_idt
  JOIN iceberg.silver.ref_unidade u ON u.unidade_cod = e.unidade_reportante_cod
  GROUP BY 1, 2, 3
),
janela AS (SELECT min(dia) AS primeiro, max(dia) AS ultimo FROM remessa),
dia AS (SELECT d FROM janela CROSS JOIN UNNEST(sequence(primeiro, ultimo, interval '1' day)) AS t(d)),
esperado AS (
  SELECT om.om_cod, dia.d AS dia, turno.turno_cod
  FROM (SELECT DISTINCT om_cod FROM remessa) om, dia,
       (SELECT * FROM (VALUES ('MATUTINO'), ('VESPERTINO')) AS t(turno_cod)) turno
)
SELECT e.om_cod, e.dia, e.turno_cod
FROM esperado e
LEFT JOIN remessa r ON r.om_cod = e.om_cod AND r.dia = e.dia AND r.turno_cod = e.turno_cod
WHERE r.om_cod IS NULL
""".strip(),
    },
    {
        "nome": "rejeitado_foi_reenviado",
        "rotulo": "Arquivo rejeitado acabou entrando na Bronze",
        "porque": "A ingestao recusa o arquivo que nao consegue ler (JSON malformado, caminho fora "
                  "da convencao) e grava a recusa em REJEICAO. Este teste pergunta se sobrou alguma "
                  "recusa SEM TRATAMENTO: o mesmo endereco que nunca entrou depois, nem como "
                  "ARQUIVO nem como RECEPCAO_BRUTA. Repare que ele se resolve sozinho — reenviado "
                  "o arquivo corrigido, o endereco passa a existir na Bronze e a linha sai da "
                  "consulta, embora a REJEICAO continue registrada (a Bronze e append-only e nao "
                  "apaga o que houve). E por isso que o teste nao precisa olhar so a ultima rodada: "
                  "ele mede pendencia, nao historico.",
        "tabela": "bronze.rejeicao",
        "definicao": "tableCustomSQLQuery",
        "sql": "SELECT r.origem_uri_txt FROM iceberg.bronze.rejeicao r "
               "LEFT JOIN iceberg.bronze.recepcao_bruta rb ON rb.origem_uri_txt = r.origem_uri_txt "
               "LEFT JOIN iceberg.bronze.arquivo a ON a.arquivo_uri_txt = r.origem_uri_txt "
               "WHERE rb.recepcao_idt IS NULL AND a.arquivo_idt IS NULL",
    },
    {
        "nome": "relper_todas_subunidades",
        "rotulo": "O RELPER que chegou trouxe todas as subunidades da OM",
        "porque": "O caso mais dificil: o arquivo chegou, foi lido e ninguem reclamou, mas parte "
                  "do conteudo se perdeu — o OCR pulou uma linha da tabela, ou a planilha veio "
                  "faltando uma companhia. So se detecta com redundancia, e a redundancia aqui e "
                  "a ordem de batalha: REF_UNIDADE diz quantas subunidades cada OM tem (as SU; "
                  "ou as fracoes diretas, quando a OM nao tem SU), logo diz quantas linhas a "
                  "planilha deveria trazer E QUAIS. A consulta devolve, para cada turno que "
                  "chegou, as subunidades que faltaram nele; zero linhas e o esperado.",
        "tabela": "silver.situacao_unidade",
        "definicao": "tableCustomSQLQuery",
        "sql": """
WITH om_com_su AS (
  SELECT DISTINCT superior_cod FROM iceberg.silver.ref_unidade WHERE escalao_cod = 'SU'
),
esperada AS (
  SELECT f.superior_cod AS om_cod, f.unidade_cod
  FROM iceberg.silver.ref_unidade f
  JOIN iceberg.silver.ref_unidade o ON o.unidade_cod = f.superior_cod AND o.escalao_cod = 'OM'
  WHERE f.escalao_cod = 'SU' OR f.superior_cod NOT IN (SELECT superior_cod FROM om_com_su)
),
remessa AS (
  SELECT u.superior_cod AS om_cod, date(e.ocorrencia_data) AS dia, s.turno_cod,
         e.unidade_reportante_cod AS unidade_cod
  FROM iceberg.silver.evento e
  JOIN iceberg.silver.situacao_unidade s ON s.evento_idt = e.evento_idt
  JOIN iceberg.silver.ref_unidade u ON u.unidade_cod = e.unidade_reportante_cod
  GROUP BY 1, 2, 3, 4
),
turno_que_chegou AS (SELECT DISTINCT om_cod, dia, turno_cod FROM remessa)
SELECT t.om_cod, t.dia, t.turno_cod, x.unidade_cod AS faltou
FROM turno_que_chegou t
JOIN esperada x ON x.om_cod = t.om_cod
LEFT JOIN remessa r ON r.om_cod = t.om_cod AND r.dia = t.dia AND r.turno_cod = t.turno_cod
                   AND r.unidade_cod = x.unidade_cod
WHERE r.unidade_cod IS NULL
""".strip(),
    },
]


# -----------------------------------------------------------------------------
# Conversa com o OpenMetadata
# -----------------------------------------------------------------------------

def _jwt() -> str:
    jwt = os.environ.get("OM_INGESTION_BOT_JWT", "")
    if not jwt:
        for linha in open(os.path.join(os.path.dirname(__file__), "..", ".env"), encoding="utf-8"):
            if linha.startswith("OM_INGESTION_BOT_JWT="):
                jwt = linha.split("=", 1)[1].strip().strip('"')
    if not jwt:
        raise SystemExit("OM_INGESTION_BOT_JWT nao encontrado (nem no ambiente, nem no .env)")
    return jwt


def _chamar(metodo: str, caminho: str, corpo=None, tipo="application/json"):
    req = urllib.request.Request(
        f"{OM_URL}/api/v1/{caminho}", method=metodo,
        data=json.dumps(corpo).encode() if corpo is not None else None,
        headers={"Content-Type": tipo, "Authorization": f"Bearer {_jwt()}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read() or "{}")


def _parametros(teste: dict) -> list:
    """O OpenMetadata recebe todo parametro como texto, inclusive listas."""
    if teste["definicao"] == "tableCustomSQLQuery":
        return [{"name": "sqlExpression", "value": teste["sql"]},
                {"name": "strategy", "value": "ROWS"},
                {"name": "operator", "value": "=="},
                {"name": "threshold", "value": "0"}]
    # matchEnum e essencial nos testes de lista: SEM ele o OpenMetadata apenas CONTA
    # quantos valores estao na lista e passa se achou algum — nao verifica que TODOS
    # estao. Com ele, cada valor e conferido, e um valor fora da lista reprova.
    def texto(valor):
        if isinstance(valor, list):
            return json.dumps(valor)
        if isinstance(valor, bool):
            return "true" if valor else "false"
        return str(valor)

    return [{"name": nome, "value": texto(valor)}
            for nome, valor in (teste.get("parametros") or {}).items()]


def _elo(teste: dict) -> str:
    """Onde o teste se prende: a tabela inteira, ou uma coluna dela."""
    alvo = f"<#E::table::{SERVICO}.{teste['tabela']}"
    return f"{alvo}::columns::{teste['coluna']}>" if teste.get("coluna") else f"{alvo}>"


def criar(teste: dict) -> str:
    corpo = {
        "name": teste["nome"],
        "displayName": teste["rotulo"],
        "description": teste["porque"],
        "entityLink": _elo(teste),
        "testDefinition": teste["definicao"],
        "parameterValues": _parametros(teste),
    }
    try:
        resposta = _chamar("POST", "dataQuality/testCases", corpo)
        return "criado  " + resposta["fullyQualifiedName"]
    except urllib.error.HTTPError as erro:
        if erro.code != 409:                     # 409 = ja existe: atualiza no lugar
            raise SystemExit(f"{teste['nome']}: HTTP {erro.code} — {erro.read().decode()[:300]}")
        resposta = _chamar("PUT", "dataQuality/testCases", corpo)
        return "atualizado " + resposta["fullyQualifiedName"]


def listar():
    tudo = _chamar("GET", "dataQuality/testCases?limit=100&fields=testDefinition,testCaseResult")
    for caso in tudo.get("data", []):
        resultado = (caso.get("testCaseResult") or {}).get("testCaseStatus", "—")
        print(f"  {resultado:9s} {caso['name']:38s} {caso['entityLink']}")
    print(f"  ({len(tudo.get('data', []))} testes)")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--listar", action="store_true", help="mostra os testes que ja existem")
    if p.parse_args().listar:
        return listar()
    for teste in TESTES:
        print(" ", criar(teste))
    print(f"\n{len(TESTES)} testes declarados. Para executa-los:\n"
          f"  docker exec dlh_airflow_scheduler airflow dags trigger 5_governanca")


if __name__ == "__main__":
    main()
