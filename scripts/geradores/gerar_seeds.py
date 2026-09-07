#!/usr/bin/env python3
"""
gerar_seeds.py — Produz as tabelas de referencia de canonico/seeds/.

Sao dimensoes conformadas: mudam pouco e sao consultadas pelas transformacoes
`referencia` e `gazetteer` do modelo canonico. Ficam num script, e nao escritas
a mao, por um motivo so: COERENCIA GEOGRAFICA. Se 'BR-154 km 43' e 'km 44'
ficarem a 300 km um do outro, toda consulta espacial vira ruido.

SIGILO
  Nenhuma unidade, localidade ou codinome aqui existe. As designacoes seguem a
  estrutura doutrinaria (para que a hierarquia seja plausivel) mas a numeracao e
  ficticia: o Exercito Brasileiro nao possui 51a Brigada. Os toponimos formam
  uma familia sintetica — nomes de arvores do cerrado — para que fique evidente
  que sao inventados. A regiao geografica e real e pouco povoada, o que da
  coordenadas plausiveis sem descrever movimento de tropa em lugar identificavel.

SIGLAS
  Compostas pela regra 3.1.5 do MD33-M-02: a abreviatura de uma expressao e o
  conjunto das abreviaturas das palavras que a constituem, separadas por espaco
  (Btl + Inf + Mtz = 'Btl Inf Mtz'). Cada componente foi conferido contra
  canonico/ir_14_06/abreviaturas_md33.csv.

    python3 scripts/geradores/gerar_seeds.py
"""

import csv
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
DIR = RAIZ / "canonico" / "seeds"

# --- ancora geografica: area rural de cerrado, pouco povoada -----------------
LAT0, LON0 = -14.10, -46.62      # inicio da BR-154 (ficticia)
GRAU_LAT_KM = 0.008993           # 1 km em graus de latitude
GRAU_LON_KM = 0.009290           # 1 km em graus de longitude nesta latitude

CABECALHO = (
    "# {titulo}\n"
    "# Gerado por scripts/geradores/gerar_seeds.py — nao editar a mao.\n"
    "# Dado SINTETICO. Nenhuma unidade, localidade ou codinome aqui existe.\n"
)


def _escrever(nome, titulo, colunas, linhas):
    caminho = DIR / nome
    with caminho.open("w", encoding="utf-8", newline="") as fh:
        fh.write(CABECALHO.format(titulo=titulo))
        w = csv.writer(fh)
        w.writerow(colunas)
        w.writerows(linhas)
    print(f"  {caminho.relative_to(RAIZ)}: {len(linhas)} linhas")


# =============================================================================
def gerar_unidades():
    """Uma brigada de infantaria motorizada completa, quatro escaloes."""
    u = []
    add = lambda cod, nome, sigla, esc, sup, tipo=None: u.append([cod, nome, sigla, esc, sup])

    add("BDA51", "51a Brigada de Infantaria Motorizada", "51a Bda Inf Mtz", "BDA", "", "INFANTARIA")

    # organizacoes militares subordinadas
    oms = [
        ("BI511", "511o Batalhao de Infantaria Motorizado", "511o Btl Inf Mtz", "INFANTARIA", "cia_fuz"),
        ("BI512", "512o Batalhao de Infantaria Motorizado", "512o Btl Inf Mtz", "INFANTARIA", "cia_fuz"),
        ("BI513", "513o Batalhao de Infantaria Motorizado", "513o Btl Inf Mtz", "INFANTARIA", "cia_fuz"),
        ("GAC51", "51o Grupo de Artilharia de Campanha", "51o GAC", "ARTILHARIA", "bia"),
        ("ENG51", "51a Companhia de Engenharia de Combate", "51a Cia Eng Cmb", "ENGENHARIA", "pel_direto"),
        ("CAV51", "51o Esquadrao de Cavalaria Mecanizado", "51o Esqd Cav Mec", "CAVALARIA", "pel_direto"),
        ("CMD51", "51a Companhia de Comando", "51a Cia Cmdo", "COMANDO", "pel_cmdo"),
    ]
    for cod, nome, sigla, tipo, forma in oms:
        add(cod, nome, sigla, "OM", "BDA51", tipo)

        if forma == "cia_fuz":
            for i in (1, 2, 3):
                su = f"{cod}C{i}"
                add(su, f"{i}a Companhia de Fuzileiros do {sigla}", f"{i}a Cia Fuz/{sigla}", "SU", cod, tipo)
                for j in (1, 2, 3):
                    add(f"{su}P{j}", f"{j}o Pelotao de Fuzileiros da {i}a Cia Fuz/{sigla}",
                        f"{j}o Pel Fuz/{i}a Cia Fuz/{sigla}", "FR", su, tipo)
            su = f"{cod}CAP"
            add(su, f"Companhia de Comando e Apoio do {sigla}", f"Cia Cmdo Ap/{sigla}", "SU", cod, "APOIO")
            add(f"{su}PM", f"Pelotao de Morteiros da Cia Cmdo Ap/{sigla}", f"Pel Mrt/{sigla}", "FR", su, "APOIO")
            add(f"{su}PA", f"Pelotao de Apoio da Cia Cmdo Ap/{sigla}", f"Pel Ap/{sigla}", "FR", su, "APOIO")

        elif forma == "bia":
            for i in (1, 2, 3):
                su = f"{cod}B{i}"
                add(su, f"{i}a Bateria de Obuses do {sigla}", f"{i}a Bia O/{sigla}", "SU", cod, tipo)
                for j in (1, 2):
                    add(f"{su}P{j}", f"{j}o Pelotao de Obuses da {i}a Bia O/{sigla}",
                        f"{j}o Pel O/{i}a Bia O/{sigla}", "FR", su, tipo)
            su = f"{cod}BC"
            add(su, f"Bateria de Comando do {sigla}", f"Bia Cmdo/{sigla}", "SU", cod, "COMANDO")
            add(f"{su}POBS", f"Pelotao de Observacao da Bia Cmdo/{sigla}", f"Pel Obs/{sigla}", "FR", su, tipo)

        elif forma == "pel_direto":
            rot = "Pel Eng Cmb" if tipo == "ENGENHARIA" else "Pel Cav Mec"
            ext = "Pelotao de Engenharia de Combate" if tipo == "ENGENHARIA" else "Pelotao de Cavalaria Mecanizado"
            for j in (1, 2, 3):
                add(f"{cod}P{j}", f"{j}o {ext} da {sigla}", f"{j}o {rot}/{sigla}", "FR", cod, tipo)

        elif forma == "pel_cmdo":
            add(f"{cod}PCOM", f"Pelotao de Comunicacoes da {sigla}", f"Pel Com/{sigla}", "FR", cod, "COMUNICACOES")
            add(f"{cod}PCMD", f"Pelotao de Comando da {sigla}", f"Pel Cmdo/{sigla}", "FR", cod, "COMANDO")

    _escrever("unidades.csv", "REF_UNIDADE — hierarquia de organizacoes militares (FICTICIA)",
              ["UNIDADE_COD", "UNIDADE_NOME", "UNIDADE_SIGLA", "ESCALAO_COD", "SUPERIOR_COD"], u)


# =============================================================================
# Toponimos: arvores do cerrado. Familia sintetica, deliberadamente reconhecivel
# como invencao, para nao colidir com nome de lugar real.
ARVORES = ["Aroeira", "Sucupira", "Jatoba", "Copaiba", "Barriguda", "Pequi",
           "Macauba", "Cagaita", "Mangaba", "Baru", "Pau-Terra", "Gonçalo-Alves"]


def _ponto(lat, lon):
    return f"POINT({lon:.6f} {lat:.6f})"


def gerar_gazetteer():
    g = []
    # --- BR-154: rodovia ficticia, sentido norte-sul, marcos de 5 em 5 km ---
    for km in range(0, 121, 5):
        lat = LAT0 - km * GRAU_LAT_KM
        g.append([f"BR154K{km:03d}", f"BR-154 km {km}", "RODOVIA_KM",
                  f"BR-154 km {km}", _ponto(lat, LON0), 250])

    # --- VC-231: vicinal leste-oeste, cruza a BR-154 no km 45 ---
    lat_cruz = LAT0 - 45 * GRAU_LAT_KM
    for km in range(0, 41, 5):
        lon = LON0 + (km - 20) * GRAU_LON_KM
        g.append([f"VC231K{km:03d}", f"VC-231 km {km}", "RODOVIA_KM",
                  f"VC-231 km {km}", _ponto(lat_cruz, lon), 250])

    # --- localidades, distribuidas ao longo dos eixos ---
    for i, arv in enumerate(ARVORES):
        km = 8 + i * 9
        lat = LAT0 - km * GRAU_LAT_KM
        lon = LON0 + (GRAU_LON_KM * (6 if i % 2 else -6))
        rot = "Vila" if i % 3 == 0 else ("Povoado" if i % 3 == 1 else "Nucleo")
        g.append([f"LOC{i:02d}", f"{rot} {arv}", "LOCALIDADE",
                  f"{rot} {arv}", _ponto(lat, lon), 1500])

    # --- pontos notaveis, ancorados em marcos ja definidos ---
    notaveis = [
        ("PN01", "Entroncamento BR-154 / VC-231", 45, 0),
        ("PN02", "Ponte sobre o Corrego Aroeira", 22, 0),
        ("PN03", "Ponte sobre o Corrego Pequi", 63, 0),
        ("PN04", "Morro do Jatoba", 31, 4),
        ("PN05", "Morro da Barriguda", 88, -5),
        ("PN06", "Vau do Corrego Macauba", 74, 3),
        ("PN07", "Fazenda Sucupira", 17, -7),
        ("PN08", "Fazenda Pau-Terra", 96, 6),
        ("PN09", "Curva do Baru", 52, 1),
        ("PN10", "Serra da Cagaita", 108, -8),
        ("PN11", "Represa da Copaiba", 39, 5),
        ("PN12", "Posto Mangaba", 12, 2),
    ]
    for cod, nome, km, desloc in notaveis:
        lat = LAT0 - km * GRAU_LAT_KM
        lon = LON0 + desloc * GRAU_LON_KM
        g.append([cod, nome, "PONTO_NOTAVEL", nome, _ponto(lat, lon), 400])

    # --- quadriculas da carta militar, malha de 10 km ---
    for li in range(4):
        for co in range(3):
            lat = LAT0 - (15 + li * 30) * GRAU_LAT_KM
            lon = LON0 + (co - 1) * 10 * GRAU_LON_KM
            cod = f"QD{44 + co}{71 + li}"
            g.append([cod, f"Quadricula {cod[2:]}", "QUADRICULA",
                      f"quadricula {cod[2:]}", _ponto(lat, lon), 5000])

    _escrever("gazetteer.csv", "REF_GAZETTEER — indice de nomes geograficos (SINTETICO)",
              ["LOCAL_COD", "LOCAL_NOME", "LOCAL_TIPO_COD", "REFERENCIA_TEXTO",
               "GEOMETRIA_WKT", "PRECISAO_METRO"], g)


# =============================================================================
if __name__ == "__main__":
    DIR.mkdir(parents=True, exist_ok=True)
    print("Gerando seeds:")
    gerar_unidades()
    gerar_gazetteer()
