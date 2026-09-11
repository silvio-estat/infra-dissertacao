#!/usr/bin/env python3
"""
testes_qualidade.py — declara no OpenMetadata os testes de qualidade da camada Bronze.

Sete testes, escolhidos para MOSTRAR O QUE A PLATAFORMA FAZ — nao para cobrir o
modelo canonico inteiro (que renderia 76 testes estruturais e nenhum leitor).
Seis passam; o quinto falha de proposito, por causa de um defeito plantado pelo
gerador: uma planilha foi remetida na pasta do ano errado.

Criar um teste por aqui e o mesmo que cria-lo pela tela do OpenMetadata: a tela e
apenas um cliente desta API. O teste aparece na aba Data Quality da tabela, com
botao de rodar e historico. A diferenca e que aqui ele fica VERSIONADO no
repositorio — quem repetir o experimento tem os mesmos testes, e um reset do
OpenMetadata nao os leva embora.

    python3 scripts/testes_qualidade.py            # cria (ou atualiza) os sete
    python3 scripts/testes_qualidade.py --listar   # so mostra o que existe hoje

Depois de criar, quem EXECUTA os testes e a DAG dag_trino_governance (tarefa
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
# OS SETE TESTES
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
                  "A consulta devolve as linhas orfas; zero linhas e o esperado.",
        "tabela": "bronze.extracao",
        "definicao": "tableCustomSQLQuery",
        "sql": "SELECT e.extracao_idt FROM iceberg.bronze.extracao e "
               "LEFT JOIN iceberg.bronze.arquivo a ON a.arquivo_idt = e.arquivo_idt "
               "WHERE a.arquivo_idt IS NULL",
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
          f"  docker exec dlh_airflow_scheduler airflow dags trigger dag_trino_governance")


if __name__ == "__main__":
    main()
