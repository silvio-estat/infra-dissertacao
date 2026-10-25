#!/usr/bin/env python3
"""
medir_acerto_leitura.py — mede o acerto das leituras de voz e de documento escaneado.

Complementa medir_acerto_relato.py (o relato do C2_B). Compara o que a Bronze e a
Silver gravaram com o gabarito do gerador, dados_sinteticos/verdade/, que o
pipeline nunca le.

O que se mede
  Transcricao (vocab-v1, faster-whisper), sobre os 150 audios:
    - exatas: transcricao igual a frase do gabarito, sem pontuacao, acento e caixa;
    - WER global: palavras erradas sobre palavras da frase (distancia de edicao);
    - codinome e toponimo PRESENTES no texto. E o que o de/para consome; o WER e
      dominado pelo indicativo e pelas palavras de protocolo (ver ACHADOS.md).
  Leitura da transcricao (voz-v2, modelo de linguagem):
    - codinome, lugar e prioridade devolvidos iguais aos ditos. O lugar casa SEM
      diferenciar caixa, como na Silver: 'fazenda buriti' vale 'Fazenda Buriti'.
  Silver VOZ: TIPO_COD do evento igual ao tipo do codinome dito.
  Docling:
    - RELPER: cada numero da planilha comparado com o gemeo escaneado (os pares
      estao em gabarito.csv), na mesma linha (presente) e na mesma coluna (celula certa);
    - FOGOS: a coordenada de cada fato na coluna 'Area Bombardeada'.

Regras de comparacao que ja enganaram (ACHADOS.md, 'Medir errado')
  - 'quilometro' (como a voz fala) = 'km' (como o gazetteer escreve);
  - coluna do Docling pela POSICAO, nunca pelo texto do cabecalho, que o OCR troca;
  - lugar com caixa diferente nao e erro: a Silver casa sem caixa.

O corpus e sintetico, com gabarito conhecido: mostra que a extracao faz o que
declara, nao o desempenho em documento real. O som dos .wav muda a cada
regeracao, entao os numeros da voz valem para a geracao medida.

Precisa do venv dos geradores (yaml, openpyxl e scripts/geradores/comum.py) e da stack no ar:
    ./venv-geradores/bin/python scripts/medir_acerto_leitura.py
"""
import csv
import json
import re
import subprocess
import sys
import unicodedata
from pathlib import Path

import openpyxl
import yaml

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "scripts" / "geradores"))
from comum import decametrica  # noqa: E402

VERDADE = RAIZ / "dados_sinteticos" / "verdade"
PRIORIDADES = {"relampago": "URGENTE", "chuva": "PRIORITARIO", "orvalho": "ROTINA"}
COLUNA_AREA_BOMBARDEADA = 5     # posicao na 1a parte do Relatorio de Bombardeio (coluna F)


def trino(sql: str) -> list:
    r = subprocess.run(["docker", "exec", "dlh_trino", "trino", "--server", "localhost:8090",
                        "--output-format", "JSON", "--execute", sql], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"Trino recusou a consulta:\n{r.stderr[-500:]}")
    return [json.loads(linha) for linha in r.stdout.splitlines() if linha.startswith("{")]


def extracoes(filtro: str) -> list:
    """[{nome, versao, saida}] das extracoes por arquivo, com o nome do arquivo sem a pasta."""
    return trino(f"""
        SELECT regexp_extract(a.arquivo_uri_txt, '[^/]+$') AS nome, e.prompt_versao_cod AS versao, e.saida_txt AS saida
        FROM iceberg.bronze.extracao e JOIN iceberg.bronze.arquivo a ON a.arquivo_idt = e.arquivo_idt
        WHERE {filtro}""")


def normalizar(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", txt.lower()).encode("ascii", "ignore").decode()
    txt = txt.replace("quilometro", "km").replace("-", " ")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", txt)).strip()


def erros_de_palavra(ref: str, hip: str) -> tuple:
    """(palavras erradas, palavras da referencia): distancia de edicao por palavra."""
    r, h = ref.split(), hip.split()
    d = list(range(len(h) + 1))
    for i in range(1, len(r) + 1):
        diag, d[0] = d[0], i
        for j in range(1, len(h) + 1):
            diag, d[j] = d[j], min(d[j] + 1, d[j - 1] + 1, diag + (r[i - 1] != h[j - 1]))
    return d[len(h)], len(r)


def pct(a: int, b: int) -> str:
    return f"{a}/{b} ({100 * a / b:.1f}%)"


def medir_voz(gabarito: list, codinomes: dict, lugares: set):
    trans, leitura = {}, {}
    for r in extracoes("a.modalidade_cod = 'AUDIO'"):
        (trans if r["versao"] == "vocab-v1" else leitura)[r["nome"]] = json.loads(r["saida"])
    tipo_silver = {r["nome"]: r["tipo"] for r in trino("""
        SELECT regexp_extract(a.arquivo_uri_txt, '[^/]+$') AS nome, e.tipo_cod AS tipo
        FROM iceberg.silver.evento e JOIN iceberg.bronze.arquivo a ON a.arquivo_idt = e.arquivo_idt
        WHERE e.sistema_origem_cod = 'VOZ'""")}

    # o nome mais longo primeiro: 'Morro do Jatoba' antes de um eventual 'Jatoba'
    por_tamanho = sorted(lugares, key=len, reverse=True)
    lugares_sem_caixa = {l.lower() for l in lugares}
    c = dict.fromkeys(["n", "exatas", "erradas", "palavras", "cod_t", "lug_t", "cod_l", "lug_l", "fora",
                       "prio_certa", "prio_errada", "com_tipo", "tipo_certo"], 0)
    for g in (x for x in gabarito if x["fonte"] == "VOZ"):
        nome, frase = g["arquivo"].split("/")[-1], normalizar(g["valor"])
        cod = next(s for s in codinomes if re.search(rf"\b{s}", frase))
        lugar = next(l for l in por_tamanho if normalizar(l) in frase)
        prio = next(p for p in PRIORIDADES if re.search(rf"\b{p}\b", frase))
        texto = normalizar(trans[nome]["texto"])
        e, p = erros_de_palavra(frase, texto)
        c["n"] += 1; c["exatas"] += texto == frase; c["erradas"] += e; c["palavras"] += p
        c["cod_t"] += re.search(rf"\b{cod}", texto) is not None
        c["lug_t"] += normalizar(lugar) in texto

        r = leitura[nome]
        lido = (r.get("referencia_local") or "").lower()
        c["cod_l"] += r.get("codinome") == cod
        c["lug_l"] += lido == lugar.lower()
        c["fora"] += lido != "" and lido not in lugares_sem_caixa
        c["prio_certa"] += r.get("prioridade") == prio
        c["prio_errada"] += r.get("prioridade") not in (None, prio)

        if tipo_silver.get(nome):
            c["com_tipo"] += 1
            c["tipo_certo"] += tipo_silver[nome] == codinomes[cod]

    n = c["n"]
    print(f"TRANSCRICAO vocab-v1 ({n} audios)")
    print(f"  exatas {c['exatas']}/{n}   WER {100 * c['erradas'] / c['palavras']:.1f}%   "
          f"codinome {pct(c['cod_t'], n)}   toponimo {pct(c['lug_t'], n)}")
    print("LEITURA voz-v2")
    print(f"  codinome {pct(c['cod_l'], n)}   toponimo {pct(c['lug_l'], n)}   lugar fora do gazetteer {c['fora']}   "
          f"prioridade certa {c['prio_certa']}, errada {c['prio_errada']}, nula {n - c['prio_certa'] - c['prio_errada']}")
    print("SILVER VOZ")
    print(f"  com tipo {c['com_tipo']}/{n}   tipo certo {c['tipo_certo']}   errado {c['com_tipo'] - c['tipo_certo']}")


def numero(x):
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x).replace(",", "."))
    except (TypeError, ValueError):
        return None


def medir_docling(gabarito: list, fatos: list):
    lido = {r["nome"]: json.loads(r["saida"]) for r in extracoes("e.prompt_versao_cod LIKE 'docling%'")}

    total = presentes = na_celula = 0
    for g in (x for x in gabarito if x["chave"] == "planilha_equivalente"):
        planilha = list(openpyxl.load_workbook(RAIZ / "dados_sinteticos" / g["valor"], data_only=True)
                        .active.iter_rows(values_only=True))
        inicio = next(i for i, linha in enumerate(planilha) if linha[0] == "Fração") + 1
        grade = [linha for tabela in lido[g["arquivo"].split("/")[-1]]["abas"].values() for linha in tabela]
        for k, linha in enumerate(planilha[inicio:]):
            # a linha do escaneado e a de mesma fracao; se o OCR estragou a sigla, a de mesma posicao
            mesma = [l for l in grade if l and normalizar(str(l[0])) == normalizar(str(linha[0]))]
            gemea = mesma[0] if mesma else (grade[k + 1] if k + 1 < len(grade) else [])
            valores = [numero(c) for c in gemea]
            for j, v in enumerate(linha):
                if j == 0 or isinstance(v, str) or numero(v) is None:
                    continue
                total += 1
                presentes += numero(v) in valores
                na_celula += j < len(valores) and valores[j] == numero(v)
    print("DOCLING RELPER (pares planilha x escaneado)")
    print(f"  numeros presentes {pct(presentes, total)}   celula certa {pct(na_celula, total)}")

    linhas_fogos = [l for nome, d in lido.items() if nome.startswith("relatorio_bombardeio")
                    for tabela in d["abas"].values() for l in tabela]
    de_fogos = [f for f in fatos if "FOGOS" in f["fontes"]]
    achados = sum(any(len(l) > COLUNA_AREA_BOMBARDEADA and l[COLUNA_AREA_BOMBARDEADA] == alvo for l in linhas_fogos)
                  for alvo in (decametrica(float(f["lat"]), float(f["lon"])) for f in de_fogos))
    print("DOCLING FOGOS")
    print(f"  fatos com a coordenada na coluna Area Bombardeada {achados}/{len(de_fogos)}")


def main():
    modelo = yaml.safe_load(open(RAIZ / "canonico" / "modelo_canonico.yaml", encoding="utf-8"))
    codinomes = {s: tipo for tipo, v in modelo["dominios"]["tipo_evento"]["valores"].items()
                 for s in (v.get("sinonimos") or [])}
    with open(RAIZ / "canonico" / "seeds" / "gazetteer.csv", encoding="utf-8") as fh:
        lugares = {l["LOCAL_NOME"] for l in csv.DictReader(x for x in fh if not x.startswith("#"))}
    gabarito = list(csv.DictReader(open(VERDADE / "gabarito.csv", encoding="utf-8")))
    fatos = list(csv.DictReader(open(VERDADE / "fatos.csv", encoding="utf-8")))
    medir_voz(gabarito, codinomes, lugares)
    medir_docling(gabarito, fatos)


if __name__ == "__main__":
    main()
