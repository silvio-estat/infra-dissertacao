#!/usr/bin/env python3
"""
extrair_abreviaturas_md33.py — Converte o Capitulo IV do MD33-M-02 (Manual de
Abreviaturas, Siglas, Simbolos e Convencoes Cartograficas das Forcas Armadas,
3a Edicao/2008) no Dicionario de Abreviaturas exigido pelo IR 14-06, Art. 32.

  IR 14-06, Art. 32: "Inicialmente, as primeiras abreviaturas a serem catalogadas no
  Dicionario de Abreviaturas serao as que constam no C 21-30."

O MD33-M-02 e a publicacao conjunta do Ministerio da Defesa que consolida esse material.

O PDF de origem fica em outras_info/ (fora do Git). Este script existe para que a
proveniencia do CSV seja auditavel: qualquer um pode reexecutar e obter o mesmo arquivo.

Regras do proprio manual usadas como criterio de parsing:
  3.1.4  siglas e abreviaturas nao tem pontuacao nem acentuacao  -> descarta falso positivo
  3.1.9  inicial maiuscula entre parenteses = abreviavel por ela quando em conjunto
  3.1.17 duplicidade de simbologia e mantida  -> a abreviatura NAO e chave unica

Requer: pdftotext (poppler-utils).

    python3 scripts/governanca/extrair_abreviaturas_md33.py \
        --pdf outras_info/manual_abreviaturas.pdf
"""

import argparse
import csv
import re
import subprocess
import sys
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
SAIDA_PADRAO = RAIZ / "canonico" / "ir_14_06" / "abreviaturas_md33.csv"

# A largura da tabela muda de pagina para pagina: em paginas de termos curtos a coluna
# "Abreviaturas/Siglas" comeca antes. Por isso o limiar e lido do cabecalho de cada
# pagina, e nao fixado. O valor abaixo so vale ate o primeiro cabecalho aparecer.
COL_ABREV_PADRAO = 45
TOLERANCIA_COL = 5          # cabecalho e coluna de dados coincidem dentro de +-2
ACENTOS = re.compile(r"[áàâãéêíóôõúüçÁÀÂÃÉÊÍÓÔÕÚÜÇ]")
MARCA_INICIAL = re.compile(r"\(([A-Z])\)")   # Art. 3.1.9
SO_CONJUNTO = re.compile(r"^(\S+)\s*\(somente em conjunto\)$")   # Art. 3.1.10


def _eh_mobilia(t: str) -> bool:
    """Cabecalho, rodape, numero de pagina e titulo de secao — nao sao dados."""
    return bool(
        not t
        or re.fullmatch(r"\d+/334", t)
        or t == "MD33-M-02"
        or re.fullmatch(r"4\.\d+\s+[A-ZÀ-Ú]", t)
        or t.startswith(("Palavras e Expres", "Abreviatura", "CAPÍTULO", "CODIFICAÇÃO"))
    )


def _parece_abreviatura(s: str) -> bool:
    """MD33-M-02, 3.1.4: sem pontuacao, sem acentuacao. Filtra continuacao de termo."""
    if SO_CONJUNTO.match(s):
        return True     # 3.1.10: abreviatura de uma letra, valida so em conjunto
    return (
        bool(s)
        and not ACENTOS.search(s)
        and not s.endswith((",", ";"))
        and len(s) <= 20
        and len(s.split()) <= 4
    )


def _separar(linha: str, col_min: int) -> tuple[str, str | None]:
    """Divide a linha em (termo, abreviatura). Devolve abreviatura None se nao houver."""
    # caso normal: intervalo de 2+ espacos com a cauda na coluna da abreviatura
    for m in reversed(list(re.finditer(r"\s{2,}", linha))):
        cauda = linha[m.end():].strip()
        if m.end() >= col_min and _parece_abreviatura(cauda):
            return linha[: m.start()].strip(), cauda
    # Termo tao largo quanto a coluna: a abreviatura cola por UM unico espaco.
    # Corta no espaco mais a esquerda cuja cauda ja caia na coluna da abreviatura,
    # para nao fatiar abreviatura composta ("P Asst Mil"). Exige inicial maiuscula:
    # pela regra 3.1.14 as siglas se formam por iniciais maiusculas, o que distingue
    # a abreviatura de um resto de termo ("... Estimulada da").
    for m in re.finditer(r"\s+", linha.rstrip()):
        cauda = linha[m.end():].strip()
        if m.end() >= col_min and cauda[:1].isupper() and _parece_abreviatura(cauda):
            return linha[: m.start()].strip(), cauda
    return linha.strip(), None


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def extrair_texto(pdf: Path) -> list[str]:
    with tempfile.NamedTemporaryFile(suffix=".txt") as tmp:
        subprocess.run(
            ["pdftotext", "-layout", str(pdf), tmp.name],
            check=True, capture_output=True,
        )
        return Path(tmp.name).read_text(encoding="utf-8").split("\n")


def recortar_capitulo_iv(linhas: list[str]) -> list[str]:
    """Isola o Capitulo IV (codificacao), entre os titulos do Cap IV e do Cap V."""
    ini = fim = None
    for i, ln in enumerate(linhas):
        t = ln.strip()
        if t == "CAPÍTULO IV" and ini is None and i > 200:
            ini = i
        elif t == "CAPÍTULO V" and ini is not None:
            fim = i
            break
    if ini is None or fim is None:
        raise SystemExit("Nao localizei os limites do Capitulo IV no PDF.")
    return linhas[ini:fim]


def analisar(linhas: list[str]) -> list[tuple[str, str]]:
    """Percorre o capitulo montando os pares (termo, abreviatura)."""
    entradas: list[list[str]] = []
    buffer: list[str] = []
    aguardando_cauda = False
    col_min = COL_ABREV_PADRAO

    for linha in linhas:
        t = linha.strip()
        # o cabecalho de cada pagina revela onde comeca a coluna da abreviatura
        if "Abreviaturas/Siglas" in linha and "Palavras" in linha:
            col_min = max(0, linha.index("Abreviaturas/Siglas") - TOLERANCIA_COL)
            continue
        if _eh_mobilia(t):
            continue
        termo, abrev = _separar(linha, col_min)

        if abrev is not None:
            # linha contendo SO a abreviatura: o termo prossegue na linha seguinte
            so_abrev = not termo
            entradas.append([_norm(" ".join(buffer + [termo])), _norm(abrev)])
            buffer, aguardando_cauda = [], so_abrev
        elif aguardando_cauda and entradas:
            entradas[-1][0] = _norm(entradas[-1][0] + " " + t)
            aguardando_cauda = False
        else:
            buffer.append(t)

    if buffer:
        print(f"aviso: {len(buffer)} linha(s) sem abreviatura ao final: {buffer[:3]}", file=sys.stderr)
    return [(t, a) for t, a in entradas if t and a]


def desdobrar(termo: str) -> tuple[str, list[str], str]:
    """Separa termo principal, sinonimos e a inicial de uso em conjunto (Art. 3.1.9).

    'Abastecimento, Abastecer, Abastecedor (A)' -> ('Abastecimento',
                                                    ['Abastecer', 'Abastecedor'], 'A')
    """
    inicial = ""
    m = MARCA_INICIAL.search(termo)
    if m:
        inicial = m.group(1)
        termo = MARCA_INICIAL.sub("", termo)

    # divide por virgula, mas nao dentro de parenteses (expansoes em idioma estrangeiro)
    partes, nivel, atual = [], 0, ""
    for ch in termo:
        if ch == "(":
            nivel += 1
        elif ch == ")":
            nivel = max(0, nivel - 1)
        if ch == "," and nivel == 0:
            partes.append(atual)
            atual = ""
        else:
            atual += ch
    partes.append(atual)

    limpos = [_norm(p) for p in partes if _norm(p)]
    if not limpos:
        return "", [], inicial
    # "Reconhecimento, Escolha e Ocupacao de Posicao" e UM termo composto, nao uma
    # lista de flexoes: a conjuncao no ultimo membro denuncia a enumeracao unica.
    if any(" e " in p for p in limpos[1:]):
        return _norm(termo), [], inicial
    return limpos[0], limpos[1:], inicial


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pdf", type=Path, default=RAIZ / "outras_info" / "manual_abreviaturas.pdf")
    parser.add_argument("--saida", type=Path, default=SAIDA_PADRAO)
    args = parser.parse_args()

    if not args.pdf.exists():
        raise SystemExit(f"PDF nao encontrado: {args.pdf}")

    entradas = analisar(recortar_capitulo_iv(extrair_texto(args.pdf)))
    print(f"{len(entradas)} entradas extraidas do Capitulo IV")

    args.saida.parent.mkdir(parents=True, exist_ok=True)
    with args.saida.open("w", encoding="utf-8", newline="") as fh:
        fh.write(
            "# Dicionario de Abreviaturas — IR 14-06, Art. 32 a 35\n"
            "# Fonte: MD33-M-02, Manual de Abreviaturas, Siglas, Simbolos e Convencoes\n"
            "#        Cartograficas das Forcas Armadas, 3a Edicao/2008, Capitulo IV.\n"
            "# Gerado por scripts/governanca/extrair_abreviaturas_md33.py — nao editar a mao.\n"
            "# inicial_conjunto: MD33-M-02, 3.1.9 — abreviavel por essa inicial quando em conjunto.\n"
            "# A abreviatura NAO e chave unica: 3.1.17 preve duplicidade de simbologia.\n"
        )
        escritor = csv.writer(fh)
        escritor.writerow(
            ["abreviatura", "termo", "sinonimos", "termo_completo",
             "inicial_conjunto", "somente_conjunto", "origem"]
        )
        for termo_bruto, abrev in entradas:
            principal, sinonimos, inicial = desdobrar(termo_bruto)
            if not principal:
                continue
            # 3.1.10: abreviatura de uma letra so vale acompanhada de outra
            m = SO_CONJUNTO.match(abrev)
            so_conjunto = "sim" if m else ""
            if m:
                abrev = m.group(1)
            # o marcador do Art. 3.1.9 as vezes cai na coluna da abreviatura ("B (C)")
            m2 = re.match(r"^(.*?)\s*\(([A-Z])\)$", abrev)
            if m2 and m2.group(1):
                abrev = m2.group(1).strip()
                inicial = inicial or m2.group(2)
            # termo_completo preserva a grafia da fonte; o desdobramento e derivado
            escritor.writerow(
                [abrev, principal, ";".join(sinonimos), _norm(termo_bruto),
                 inicial, so_conjunto, "MD33-M-02"]
            )

    print(f"gravado: {args.saida.relative_to(RAIZ)}")


if __name__ == "__main__":
    main()
