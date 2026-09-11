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
  uma familia sintetica — nomes de arvores nativas — para que fique evidente
  que sao inventados. A regiao geografica e real: o Vale do Paraiba, entre
  Lorena/Cruzeiro (SP) e Resende (RJ), onde ocorreu a Operacao Perseu 2024
  (exercicio publico, 25 nov a 5 dez 2024) que da contexto ao estudo. As
  rodovias, os corregos e as localidades, porem, sao inventados: a BR-154 nao
  existe na numeracao do DNIT.

GEOMETRIA
  Tudo e posicionado num eixo: a BR-154 parte de LAT0/LON0 com rumo AZIMUTE e
  cada ponto e descrito por (km ao longo do eixo, km perpendicular a ele). Da
  para mover a area inteira trocando duas constantes.

SIGLAS
  Compostas pela regra 3.1.5 do MD33-M-02: a abreviatura de uma expressao e o
  conjunto das abreviaturas das palavras que a constituem, separadas por espaco
  (Btl + Inf + Mtz = 'Btl Inf Mtz'). Cada componente foi conferido contra
  canonico/ir_14_06/abreviaturas_md33.csv.

    python3 scripts/geradores/gerar_seeds.py
"""

import csv
import math
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
DIR = RAIZ / "canonico" / "seeds"

# --- ancora geografica: Vale do Paraiba, area da Operacao Perseu 2024 --------
LAT0, LON0 = -22.80, -45.25      # km 0 da BR-154 (ficticia), a sudoeste de Lorena/SP
AZIMUTE = 68.0                   # rumo do eixo em graus (ENE), acompanhando o vale ate Resende/RJ
KM_LAT = 1 / 110.574                                        # 1 km em graus de latitude
KM_LON = 1 / (111.320 * math.cos(math.radians(LAT0)))       # 1 km em graus de longitude nesta latitude


def _latlon(km, perp=0.0):
    """Ponto a `km` ao longo do eixo da BR-154 e `perp` km ao lado dele (+ esquerda, - direita)."""
    az = math.radians(AZIMUTE)
    norte = km * math.cos(az) + perp * math.sin(az)
    leste = km * math.sin(az) - perp * math.cos(az)
    return LAT0 + norte * KM_LAT, LON0 + leste * KM_LON

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
# Toponimos: arvores nativas. Familia sintetica, deliberadamente reconhecivel
# como invencao, para nao colidir com nome de lugar real.
ARVORES = ["Aroeira", "Sucupira", "Jatoba", "Copaiba", "Barriguda", "Pequi",
           "Macauba", "Cagaita", "Mangaba", "Baru", "Pau-Terra", "Gonçalo-Alves"]


def _ponto(lat, lon):
    return f"POINT({lon:.6f} {lat:.6f})"


def gerar_gazetteer():
    g = []
    # --- BR-154: rodovia ficticia ao longo do eixo do vale, marcos de 5 em 5 km ---
    for km in range(0, 121, 5):
        g.append([f"BR154K{km:03d}", f"BR-154 km {km}", "RODOVIA_KM",
                  f"BR-154 km {km}", _ponto(*_latlon(km)), 250])

    # --- VC-231: vicinal perpendicular, cruza a BR-154 no km 45 ---
    for km in range(0, 41, 5):
        g.append([f"VC231K{km:03d}", f"VC-231 km {km}", "RODOVIA_KM",
                  f"VC-231 km {km}", _ponto(*_latlon(45, km - 20)), 250])

    # --- localidades, distribuidas ao longo dos eixos ---
    for i, arv in enumerate(ARVORES):
        km = 8 + i * 9
        rot = "Vila" if i % 3 == 0 else ("Povoado" if i % 3 == 1 else "Nucleo")
        g.append([f"LOC{i:02d}", f"{rot} {arv}", "LOCALIDADE",
                  f"{rot} {arv}", _ponto(*_latlon(km, 6 if i % 2 else -6)), 1500])

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
        g.append([cod, nome, "PONTO_NOTAVEL", nome, _ponto(*_latlon(km, desloc)), 400])

    # --- quadriculas da carta militar, malha de 10 km ---
    for li in range(4):
        for co in range(3):
            cod = f"QD{44 + co}{71 + li}"
            lat, lon = _latlon(15 + li * 30, (co - 1) * 10)
            g.append([cod, f"Quadricula {cod[2:]}", "QUADRICULA",
                      f"quadricula {cod[2:]}", _ponto(lat, lon), 5000])

    _escrever("gazetteer.csv", "REF_GAZETTEER — indice de nomes geograficos (SINTETICO)",
              ["LOCAL_COD", "LOCAL_NOME", "LOCAL_TIPO_COD", "REFERENCIA_TXT",
               "GEOMETRIA_WKT", "PRECISAO_METRO"], g)


# =============================================================================
# A operacao que delimita o estudo. Uma linha: decisao do usuario (2026-09-09).
# Perseu 2024 e um exercicio real e publico (25 nov a 5 dez 2024, Vale do
# Paraiba ate Resende); so o nome, as datas e a regiao sao reais — as unidades
# e os fatos gerados sao sinteticos.
def gerar_operacoes():
    cod = "PERSEU_2024"
    # area de operacoes: faixa de 15 km para cada lado do eixo, do km -10 ao km 130
    cantos = [_latlon(-10, 15), _latlon(130, 15), _latlon(130, -15), _latlon(-10, -15), _latlon(-10, 15)]
    area = "POLYGON((" + ", ".join(f"{lon:.6f} {lat:.6f}" for lat, lon in cantos) + "))"
    linhas = [[cod, "Operacao Perseu 2024", "ADESTRAMENTO",
               "2024-11-25T03:00:00Z", "2024-12-06T02:59:59Z", area, "BDA51"]]
    _escrever("operacoes.csv", "REF_OPERACAO — operacoes que delimitam o estudo",
              ["OPERACAO_COD", "OPERACAO_NOME", "OPERACAO_TIPO_COD",
               "INICIO_DATA", "FIM_DATA", "AREA_WKT", "UNIDADE_RESP_COD"], linhas)


# =============================================================================
if __name__ == "__main__":
    DIR.mkdir(parents=True, exist_ok=True)
    print("Gerando seeds:")
    gerar_unidades()
    gerar_gazetteer()
    gerar_operacoes()
