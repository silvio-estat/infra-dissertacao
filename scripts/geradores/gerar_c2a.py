#!/usr/bin/env python3
"""
gerar_c2a.py — a fonte C2_A: JSON com geometria nativa, sem IA.

Duas exportacoes, como o aplicativo faria:

  posicao/   localizadores em tempo real. Cada fracao (pelotao) anda pelo
             setor da sua OM ao longo do eixo da BR-154, e o aplicativo grava
             um ponto a cada `PASSO_MIN` minutos entre 06h e 22h. Um arquivo
             por dia e hora, com N registros — e uma exportacao em lote, nao
             um arquivo por ponto.
  mcc/       medidas de coordenacao e controle: linha de fase, zona de
             reuniao, limite, eixo de progressao (desenho a mao livre, muitos
             vertices), ponto de controle, objetivo — e os OBSTACULOS do
             cenario (campo de minas, cratera, barreira), que o C2_A desenha
             na mesma tela. A `especie` e o que separa MCC de OBSTACULO no
             de/para (sinonimos do dominio tipo_evento).

A operacao vai como o C2_A a identifica: chave natural nome + ano, em dois
campos (`operacao`: 'Perseu', `ano`: '2024'), formatados pelo proprio sistema.
O codigo canonico PERSEU_2024 e so NOME_ANO em maiusculas. A fracao vai
como SIGLA ('2o Pel Fuz/1a Cia Fuz/511o Btl Inf Mtz'), como o sistema exporta.

Defeito plantado: um lote de posicoes reenviado com outro nome de arquivo
(mesmo conteudo) — a deduplicacao por hash em RECEPCAO_BRUTA tem o que pegar.
"""

import math
import shutil
from datetime import timedelta

from cenario import fatos_para
from comum import Contexto, deslocar, escrever_json, geojson_ponto, id_det, iso, jitter, local
from gerar_seeds import _latlon

PASSO_MIN = 15          # intervalo entre pontos do localizador
HORA_INI, HORA_FIM = 6, 22

MCC_FUNDO = [           # (especie, geometria, peso)
    ("linha de fase", "LineString", 5),
    ("limite", "LineString", 3),
    ("zona de reuniao", "Polygon", 4),
    ("eixo de progressao", "LineString", 3),
    ("ponto de controle", "Point", 5),
    ("objetivo", "Polygon", 3),
    ("linha de partida", "LineString", 2),
]
NOMES_LF = ["AZUL", "VERDE", "PRATA", "BRONZE", "OURO", "RUBI", "JADE", "ONIX", "AMBAR", "COBRE"]


# =============================================================================
def gerar_posicoes(ctx: Contexto) -> dict:
    rng = ctx.rng
    fracoes = ctx.por_escalao("FR")
    passo = max(5, round(PASSO_MIN / max(ctx.escala, 0.1)))   # escala menor = menos pontos
    passo = min(passo, 60)
    estado = {}
    for u in fracoes:
        ini, fim = ctx.setores[ctx.om_de(u["UNIDADE_COD"])]
        estado[u["UNIDADE_COD"]] = [rng.uniform(ini + 1, fim - 1), rng.uniform(-6, 6), ini, fim]
    total, arquivos = 0, 0
    pasta = ctx.pasta("c2a", "posicao")
    for dia in ctx.operacao.dias:
        for hora in range(HORA_INI, HORA_FIM):
            registros = []
            for minuto in range(0, 60, passo):
                t = local(dia, hora, minuto, rng.randint(0, 59))
                for u in fracoes:
                    e = estado[u["UNIDADE_COD"]]
                    # passeio aleatorio ao longo do eixo (km) e ao lado dele; presa ao setor
                    e[0] = min(e[3] - 0.5, max(e[2] + 0.5, e[0] + rng.gauss(0, 0.35)))
                    e[1] = min(8, max(-8, e[1] + rng.gauss(0, 0.15)))
                    lat, lon = _latlon(e[0], e[1])
                    lat, lon = jitter(lat, lon, 0.02, rng)     # ruido do receptor
                    registros.append({
                        "id": id_det("C2A", "POS", u["UNIDADE_COD"], iso(t)),
                        "hora": iso(t),
                        "fracao": u["UNIDADE_SIGLA"],
                        "coordenadas": geojson_ponto(lat, lon),
                        "precisao_m": round(rng.uniform(3, 14), 1),
                        "velocidade_kmh": round(abs(rng.gauss(8, 6)), 1),
                        "operacao": ctx.operacao.nome_c2a, "ano": ctx.operacao.ano,
                    })
            nome = f"posicao_{dia.isoformat()}T{hora:02d}.json"
            escrever_json(pasta / nome, registros)
            total += len(registros)
            arquivos += 1
    # defeito: o mesmo lote reenviado com outro nome
    origem = pasta / f"posicao_{ctx.operacao.dias[1].isoformat()}T10.json"
    copia = pasta / f"posicao_{ctx.operacao.dias[1].isoformat()}T10_reenvio.json"
    shutil.copyfile(origem, copia)
    ctx.registrar_defeito("C2_A", copia.relative_to(ctx.saida), "arquivo_reenviado",
                          f"copia byte a byte de {origem.name}; mesmo hash, nao deve gerar linha nova")
    return {"registros": total, "arquivos": arquivos + 1}


# =============================================================================
def _geometria(ctx: Contexto, especie: str, tipo_geo: str, km: float, perp: float) -> dict:
    rng = ctx.rng

    def ll(k, p):
        lat, lon = _latlon(k, p)
        return [round(lon, 6), round(lat, 6)]

    if tipo_geo == "Point":
        return {"type": "Point", "coordinates": ll(km, perp)}
    if tipo_geo == "Polygon":
        # quadrilatero irregular de ~1-3 km
        r = rng.uniform(0.5, 1.5)
        pts = [ll(km + r * math.cos(a) * rng.uniform(0.8, 1.2), perp + r * math.sin(a) * rng.uniform(0.8, 1.2))
               for a in (0, 1.57, 3.14, 4.71)]
        return {"type": "Polygon", "coordinates": [pts + [pts[0]]]}
    # LineString
    if especie in ("linha de fase", "linha de partida"):
        return {"type": "LineString", "coordinates": [ll(km + rng.uniform(-0.3, 0.3), p) for p in (-9, -4, 0, 4, 9)]}
    if especie == "limite":
        return {"type": "LineString", "coordinates": [ll(k, perp + rng.uniform(-0.4, 0.4)) for k in (km - 8, km - 4, km, km + 4, km + 8)]}
    # eixo de progressao: desenho a mao livre, muitos vertices
    n = rng.randint(40, 120)
    pts, k, p = [], km - 6, perp
    for _ in range(n):
        k += 12 / n
        p += rng.gauss(0, 0.12)
        pts.append(ll(k, p))
    return {"type": "LineString", "coordinates": pts}


def gerar_mcc(ctx: Contexto) -> dict:
    rng = ctx.rng
    registros = []
    # 1) obstaculos do cenario — o C2_A desenha o que a tropa reportou
    for f in fatos_para(ctx, "C2A_MCC"):
        geo = "Polygon" if f.especie in ("campo de minas",) else "Point"
        if geo == "Polygon":
            lat, lon = f.lat, f.lon
            pts = [[round(lon + dx, 6), round(lat + dy, 6)] for dx, dy in
                   ((-0.003, -0.002), (0.003, -0.002), (0.003, 0.002), (-0.003, 0.002))]
            geometria = {"type": "Polygon", "coordinates": [pts + [pts[0]]]}
        else:
            geometria = geojson_ponto(f.lat, f.lon)
        registros.append({
            "id": id_det("C2A", "MCC", f.fato_id),
            "hora": iso(f.hora_dt + timedelta(minutes=rng.randint(10, 90))),
            "fracao": ctx.sigla(f.unidade_cod),
            "especie": f.especie,
            "nome": f"{f.especie.upper()} {f.local_nome.upper()}",
            "geometria": geometria,
            "observacao": f.descricao,
            "operacao": ctx.operacao.nome_c2a, "ano": ctx.operacao.ano,
        })
    # 2) medidas de coordenacao de fundo — planejamento da brigada e das OM
    n_fundo = ctx.n(200) - len(registros)
    pares = [(e, p) for e, g, p in MCC_FUNDO for _ in range(p)]
    oms = ctx.por_escalao("OM")
    for i in range(max(0, n_fundo)):
        especie = rng.choice(pares)[0]
        tipo_geo = next(g for e, g, _ in MCC_FUNDO if e == especie)
        om = rng.choice(oms)
        ini, fim = ctx.setores[om["UNIDADE_COD"]]
        km, perp = rng.uniform(ini, fim), rng.uniform(-6, 6)
        dia = rng.choice(ctx.operacao.dias)
        t = local(dia, rng.randint(5, 23), rng.randint(0, 59))
        nome = {"linha de fase": f"LF {rng.choice(NOMES_LF)}", "linha de partida": "LP",
                "limite": f"Lim {om['UNIDADE_SIGLA']}", "zona de reuniao": f"Z Reu {rng.randint(1, 9)}",
                "eixo de progressao": f"E Prog {rng.choice(NOMES_LF)}",
                "ponto de controle": f"P Ct {rng.randint(1, 30)}", "objetivo": f"O{rng.randint(1, 9)}"}[especie]
        registros.append({
            "id": id_det("C2A", "MCC", "fundo", i),
            "hora": iso(t),
            "fracao": om["UNIDADE_SIGLA"],
            "especie": especie,
            "nome": nome,
            "geometria": _geometria(ctx, especie, tipo_geo, km, perp),
            "operacao": ctx.operacao.nome_c2a, "ano": ctx.operacao.ano,
        })
    registros.sort(key=lambda r: r["hora"])
    pasta = ctx.pasta("c2a", "mcc")
    por_lote = 25
    for i in range(0, len(registros), por_lote):
        escrever_json(pasta / f"mcc_lote_{i // por_lote + 1:02d}.json", registros[i:i + por_lote])
    return {"registros": len(registros), "arquivos": math.ceil(len(registros) / por_lote)}


def gerar(ctx: Contexto) -> dict:
    r = {"posicao": gerar_posicoes(ctx), "mcc": gerar_mcc(ctx)}
    ctx.resumo["C2_A"] = r
    return r


if __name__ == "__main__":
    from gerar_tudo import executar_um
    executar_um("C2_A", gerar)
