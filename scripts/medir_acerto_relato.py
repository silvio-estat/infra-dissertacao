#!/usr/bin/env python3
"""
medir_acerto_relato.py — mede o acerto do modelo de linguagem nos relatos do C2_B.

O relato e texto livre: tipo e prioridade nao vem em campo, o modelo deduz do
texto (tarefa `relato` da DAG 2_bronze_extracao). Este script compara o que o
modelo respondeu com o gabarito do gerador, dados_sinteticos/verdade/fatos.csv.

O que se mede, e o que NAO se mede
  - Os relatos que narram um fato do cenario (fontes com C2B_RELATO) tem tipo e
    prioridade no gabarito. Sao a amostra do acerto — 22 relatos.
  - Os demais sao relatos de rotina. O gerador nao lhes da tipo; a prioridade e
    ROTINA por construcao, e so ela e conferida.
  - O corpus e sintetico, com gabarito conhecido: mostra que a extracao faz o que
    declara, nao o desempenho em documento real.

Como se acha o relato de cada fato
  O JSON do relato nao carrega o id do fato. O gerador escreve
  'As HHMM, <fracao> informa <descricao> nas proximidades de <local>.' e depois
  abrevia palavras ao acaso e as vezes poe tudo em minuscula. O par e: a hora do
  fato (em -03) no inicio + a descricao + o local, com as mesmas abreviacoes
  aplicadas dos dois lados. Cada fato precisa casar com exatamente um relato —
  senao o script para, em vez de medir errado.
  (Em 12/09 o par era so pelos 40 primeiros caracteres da descricao: dois fatos
  de mesma descricao caiam no mesmo relato e cinco ficavam sem par. Os numeros
  daquela medicao estao errados.)

Versoes apagadas
  Cada versao do prompt foi medida e apagada da tabela antes da seguinte. As
  respostas continuam nos snapshots do Iceberg e sao lidas com FOR VERSION AS OF.
  Se dag_iceberg_maintenance (hoje pausada) rodar expire_snapshots, elas somem.

    python3 scripts/medir_acerto_relato.py            # todas as versoes
    python3 scripts/medir_acerto_relato.py --erros    # + cada fato em que errou
"""
import argparse
import csv
import json
import math
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parent.parent
GABARITO = RAIZ / "dados_sinteticos" / "verdade" / "fatos.csv"
MODELO = RAIZ / "canonico" / "modelo_canonico.yaml"

# snapshot do INSERT de cada versao que ja nao esta na tabela
SNAPSHOTS = {
    "relato-v1": 5302099064065083570,   # lista de codigos, sem criterio
    "relato-v2": 218242516275008809,    # o que e o documento, tipos da fonte, quando cada um se aplica
    "relato-v3": 1291687178912460115,   # cada lista amarrada a sua chave + tres exemplos
    "relato-v4": 843852446770918492,    # raciocinio + format json: 300 respostas vazias
}

# As mesmas trocas de gerar_c2b.ABREV, na MESMA ordem: 'viatura' vem antes de
# 'viaturas', entao no texto 'viaturas' vira 'Vtrs', nunca 'Vtr'.
ABREV = [("viatura", "vtr"), ("viaturas", "vtr"), ("aproximadamente", "aprox"), ("companhia", "cia"),
         ("pelotao", "pel"), ("posicao", "pos"), ("municao", "mun"), ("forca oponente", "f op"),
         ("comandante", "cmt"), ("reforco", "ref"), ("observacao", "obs")]

# Exemplo do prompt desde a v3 que e, literalmente, a descricao de 4 fatos do
# gabarito. Acertar esses 4 nao prova nada: a coluna "sem o exemplo" os exclui.
EXEMPLO_NO_PROMPT = "viatura da fracao atolada na via"


def trino(sql: str) -> list:
    r = subprocess.run(["docker", "exec", "dlh_trino", "trino", "--server", "localhost:8090",
                        "--output-format", "JSON", "--execute", sql], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"Trino recusou a consulta:\n{r.stderr[-500:]}")
    return [json.loads(linha) for linha in r.stdout.splitlines() if linha.strip()]


def versoes() -> dict:
    """{versao: snapshot} — as que estao na tabela hoje (snapshot None) e as apagadas."""
    hoje = {l["prompt_versao_cod"]: None for l in trino(
        "SELECT DISTINCT prompt_versao_cod FROM iceberg.bronze.extracao WHERE recepcao_idt IS NOT NULL")}
    return dict(sorted({**SNAPSHOTS, **hoje}.items()))


def ler(versao: str, snapshot) -> list:
    """[(texto do relato, resposta do modelo)] — resposta {} quando a chamada falhou."""
    tabela = "iceberg.bronze.extracao" + (f" FOR VERSION AS OF {snapshot}" if snapshot else "")
    linhas = trino(f"""
        SELECT json_extract_scalar(r.conteudo_json_txt, '$.situacao') AS texto, e.saida_txt, e.status_cod
        FROM {tabela} e
        JOIN iceberg.bronze.recepcao_bruta r ON r.recepcao_idt = e.recepcao_idt
        WHERE e.prompt_versao_cod = '{versao}'""")
    return [(l["texto"], json.loads(l["saida_txt"]) if l["status_cod"] == "OK" else {}) for l in linhas]


def normalizar(txt: str) -> str:
    txt = txt.lower()
    for cheio, curto in ABREV:
        txt = txt.replace(cheio, curto)
    return txt


def parear(fatos: list, relatos: list) -> dict:
    """{fato_id: posicao do relato na lista}."""
    par = {}
    for f in fatos:
        hora = (datetime.fromisoformat(f["hora"].replace("Z", "+00:00")) - timedelta(hours=3)).strftime("%H%M")
        achados = [i for i, (texto, _) in enumerate(relatos)
                   if normalizar(texto).startswith(f"as {hora},")
                   and normalizar(f["descricao"]) in normalizar(texto)
                   and normalizar(f["local_nome"]) in normalizar(texto)]
        if len(achados) != 1 or achados[0] in par.values():
            raise SystemExit(f"{f['fato_id']}: {len(achados)} relatos casam — pareamento nao confiavel")
        par[f["fato_id"]] = achados[0]
    return par


def wilson(acertos: int, n: int, z: float = 1.96) -> tuple:
    """Intervalo de 95% para uma proporcao. Com n = 22, o de Wald (p +- 1,96 ep)
    passa de 100% perto dos extremos."""
    p = acertos / n
    centro = (p + z * z / (2 * n)) / (1 + z * z / n)
    meia = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return centro - meia, centro + meia


def main():
    ap = argparse.ArgumentParser(description="Acerto do modelo de linguagem nos relatos do C2_B")
    ap.add_argument("--erros", action="store_true", help="lista cada fato em que o modelo errou")
    args = ap.parse_args()

    fatos = [f for f in csv.DictReader(open(GABARITO, encoding="utf-8")) if "C2B_RELATO" in f["fontes"]]
    limpos = [f for f in fatos if normalizar(EXEMPLO_NO_PROMPT) not in normalizar(f["descricao"])]
    receita = yaml.safe_load(open(MODELO, encoding="utf-8"))["fontes"]["C2_B"]["entidades"]["relato"]
    tipos_da_fonte = set(receita["tipos_possiveis"])

    print(f"{len(fatos)} fatos com gabarito ({len(limpos)} sem o exemplo do prompt) | IC de Wilson 95%\n")
    print(f"{'versao':10} {'tipo':>20} {'prioridade':>20} | {'sem o exemplo':>13} | "
          f"{'rotina=ROTINA':>13} {'tipo invalido':>13} {'falhas':>6}")
    erros = []
    for versao, snapshot in versoes().items():
        relatos = ler(versao, snapshot)
        falhas = sum(1 for _, s in relatos if not s)
        if falhas == len(relatos):
            print(f"{versao:10} {'—':>20} {'—':>20} | {'—':>13} | {'—':>13} {'—':>13} {falhas:>6}")
            continue
        par = parear(fatos, relatos)

        def acertou(f, campo, gabarito):
            return (relatos[par[f["fato_id"]]][1].get(campo) or "").upper() == f[gabarito]

        def taxa(grupo, campo, gabarito, ic=True):
            a, n = sum(acertou(f, campo, gabarito) for f in grupo), len(grupo)
            if not ic:
                return f"{100 * a / n:.0f}%"
            lo, hi = wilson(a, n)
            return f"{a}/{n} {100 * a / n:3.0f}% [{100 * lo:.0f}-{100 * hi:.0f}]"

        rotina = [s for i, (_, s) in enumerate(relatos) if i not in par.values()]
        rotina_ok = sum(1 for s in rotina if s.get("PRIORIDADE_COD") == "ROTINA")
        invalido = sum(1 for _, s in relatos if s and s.get("TIPO_COD") not in tipos_da_fonte)
        print(f"{versao:10} {taxa(fatos, 'TIPO_COD', 'tipo_cod'):>20} "
              f"{taxa(fatos, 'PRIORIDADE_COD', 'prioridade_cod'):>20} | "
              f"{taxa(limpos, 'TIPO_COD', 'tipo_cod', False) + ' ' + taxa(limpos, 'PRIORIDADE_COD', 'prioridade_cod', False):>13} | "
              f"{f'{rotina_ok}/{len(rotina)}':>13} {invalido:>13} {falhas:>6}")

        for f in fatos:
            if not (acertou(f, "TIPO_COD", "tipo_cod") and acertou(f, "PRIORIDADE_COD", "prioridade_cod")):
                s = relatos[par[f["fato_id"]]][1]
                erros.append(f"  {versao} {f['fato_id']}  tipo {f['tipo_cod']} -> {s.get('TIPO_COD')}  "
                             f"prioridade {f['prioridade_cod']} -> {s.get('PRIORIDADE_COD')}  | {f['descricao']}")

    print("\ntipo invalido = nulo ou fora dos tipos_possiveis do relato no modelo canonico")
    if args.erros:
        print("\nerros (gabarito -> modelo):")
        print("\n".join(erros))


if __name__ == "__main__":
    main()
