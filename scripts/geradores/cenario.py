#!/usr/bin/env python3
"""
cenario.py — a lista de FATOS que as fontes vao relatar, cada uma do seu jeito.

Um fato e algo que aconteceu no terreno num instante e num lugar: uma viatura
da forca oponente vista perto de uma ponte, uma cratera na via, um bombardeio
sobre uma posicao. O cenario sorteia os fatos e decide QUAIS fontes o relatam:
o mesmo avistamento pode virar uma mensagem de radio (VOZ), um relato no
C2_B e um informe de inteligencia (INTEL). E isso que da a consulta de fusao
algo para juntar — e o que o modelo canonico existe para permitir.

Os fatos ficam em dados_sinteticos/verdade/fatos.csv. Servem para inspecao
e para o gerador ser coerente; NAO sao lidos pelo pipeline.
"""

import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import timedelta

from comum import Contexto, iso, jitter, local

# tipo -> (funcao de combate, peso no sorteio, fontes candidatas [a primeira e obrigatoria])
TIPOS = {
    "AVISTAMENTO_INIMIGO": ("INTELIGENCIA", 5, ["VOZ", "C2B_RELATO", "INTEL"]),
    "AVISTAMENTO":         ("INTELIGENCIA", 3, ["C2B_RELATO", "VOZ"]),
    "INCIDENTE":           ("PROTECAO",     5, ["C2B_INCIDENTE", "C2B_RELATO", "VOZ"]),
    "OBSTACULO":           ("PROTECAO",     3, ["C2A_MCC", "C2B_INCIDENTE", "VOZ"]),
    "BOMBARDEIO_INIMIGO":  ("FOGOS",        3, ["FOGOS", "VOZ", "INTEL"]),
    "MISSAO_TIRO":         ("FOGOS",        2, ["VOZ"]),
}

DESCRICOES = {
    "AVISTAMENTO_INIMIGO": [
        "viatura blindada da forca oponente em deslocamento",
        "grupo de aproximadamente {n} combatentes da forca oponente a pe",
        "posto de observacao da forca oponente instalado em elevacao",
        "coluna de {n} viaturas leves da forca oponente",
        "patrulha da forca oponente cruzando a via",
    ],
    "AVISTAMENTO": [
        "drone nao identificado sobrevoando a area",
        "viaturas civis estacionadas em ponto suspeito",
        "movimento incomum de civis na via",
        "embarcacao nao identificada no curso d'agua",
    ],
    "INCIDENTE": [
        "viatura da fracao atolada na via",
        "acidente com viatura, sem vitimas",
        "militar com lesao durante deslocamento",
        "artefato suspeito encontrado na via",
        "falha de energia no posto de comando",
        "colisao leve entre viaturas em comboio",
    ],
    "OBSTACULO": [
        "campo de minas simulado sinalizado",
        "cratera na via",
        "barreira de troncos na estrada",
        "ponte com capacidade reduzida",
        "fosso anticarro",
    ],
    "BOMBARDEIO_INIMIGO": [
        "bombardeio de artilharia da forca oponente sobre a posicao",
        "tiros de morteiro da forca oponente sobre a area",
        "fogos de artilharia inimiga de interdicao sobre a via",
    ],
    "MISSAO_TIRO": [
        "pedido de fogo sobre alvo observado",
        "missao de tiro de neutralizacao sobre posicao inimiga",
    ],
}

# obstaculo -> especie que o C2_A exporta (vira sinonimo de OBSTACULO no dominio)
ESPECIE_OBSTACULO = {
    "campo de minas simulado sinalizado": "campo de minas",
    "cratera na via": "cratera",
    "barreira de troncos na estrada": "barreira",
    "ponte com capacidade reduzida": "barreira",
    "fosso anticarro": "fosso anticarro",
}

PRIORIDADE_POR_TIPO = {
    "AVISTAMENTO_INIMIGO": [("URGENTE", 5), ("PRIORITARIO", 4), ("ROTINA", 1)],
    "AVISTAMENTO":         [("URGENTE", 1), ("PRIORITARIO", 4), ("ROTINA", 5)],
    "INCIDENTE":           [("URGENTE", 3), ("PRIORITARIO", 4), ("ROTINA", 3)],
    "OBSTACULO":           [("URGENTE", 2), ("PRIORITARIO", 5), ("ROTINA", 3)],
    "BOMBARDEIO_INIMIGO":  [("URGENTE", 7), ("PRIORITARIO", 3), ("ROTINA", 0)],
    "MISSAO_TIRO":         [("URGENTE", 5), ("PRIORITARIO", 5), ("ROTINA", 0)],
}


@dataclass
class Fato:
    fato_id: str
    tipo_cod: str
    funcao_cod: str
    hora: str                 # ISO UTC
    unidade_cod: str          # fracao ou subunidade que viu / sofreu
    om_cod: str
    local_cod: str
    local_nome: str
    lat: float
    lon: float
    prioridade_cod: str
    descricao: str
    especie: str = ""         # so para OBSTACULO: o que o C2_A exporta
    fontes: list = field(default_factory=list)

    @property
    def hora_dt(self):
        from datetime import datetime
        return datetime.fromisoformat(self.hora.replace("Z", "+00:00"))


def _sortear_ponderado(rng, pares):
    total = sum(p for _, p in pares)
    r = rng.uniform(0, total)
    acc = 0
    for v, p in pares:
        acc += p
        if r <= acc:
            return v
    return pares[-1][0]


def gerar_cenario(ctx: Contexto, n_fatos: int | None = None) -> list[Fato]:
    rng = ctx.rng
    n = n_fatos or ctx.n(60)
    dias = ctx.operacao.dias
    fracoes = ctx.por_escalao("FR") + ctx.por_escalao("SU")
    tipos = [(t, TIPOS[t][1]) for t in TIPOS]
    fatos = []
    for i in range(n):
        tipo = _sortear_ponderado(rng, tipos)
        funcao, _, candidatas = TIPOS[tipo]
        # unidade e lugar: o lugar fica no setor da OM da unidade, para o mapa fazer sentido
        if tipo in ("BOMBARDEIO_INIMIGO", "MISSAO_TIRO"):
            # quem sofre/ve bombardeio e infantaria; a artilharia e quem responde
            cand = [u for u in fracoes if u["UNIDADE_COD"].startswith("BI")]
        else:
            cand = fracoes
        u = rng.choice(cand)
        om = ctx.om_de(u["UNIDADE_COD"])
        locais = ctx.locais(om) or ctx.gazetteer
        lugar = rng.choice(locais)
        lat, lon = jitter(lugar["lat"], lugar["lon"], 0.4, rng)
        dia = rng.choice(dias)
        hora = local(dia, rng.randint(5, 22), rng.randint(0, 59), rng.randint(0, 59))
        # a primeira fonte candidata sempre relata; as demais, com probabilidade
        fontes = [candidatas[0]] + [f for f in candidatas[1:] if rng.random() < 0.55]
        desc = rng.choice(DESCRICOES[tipo]).format(n=rng.randint(3, 12))
        fatos.append(Fato(
            fato_id=f"F{i + 1:03d}", tipo_cod=tipo, funcao_cod=funcao, hora=iso(hora),
            unidade_cod=u["UNIDADE_COD"], om_cod=om, local_cod=lugar["LOCAL_COD"],
            local_nome=lugar["LOCAL_NOME"], lat=round(lat, 6), lon=round(lon, 6),
            prioridade_cod=_sortear_ponderado(rng, PRIORIDADE_POR_TIPO[tipo]),
            descricao=desc, especie=ESPECIE_OBSTACULO.get(desc, ""), fontes=fontes))
    fatos.sort(key=lambda f: f.hora)
    ctx.fatos = fatos
    return fatos


def salvar_verdade(ctx: Contexto):
    ctx.verdade.mkdir(parents=True, exist_ok=True)
    with open(ctx.verdade / "fatos.csv", "w", encoding="utf-8", newline="") as fh:
        campos = list(asdict(ctx.fatos[0]).keys())
        w = csv.DictWriter(fh, fieldnames=campos)
        w.writeheader()
        for f in ctx.fatos:
            d = asdict(f)
            d["fontes"] = "|".join(d["fontes"])
            w.writerow(d)
    with open(ctx.verdade / "defeitos.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["fonte", "arquivo", "tipo", "descricao"])
        w.writeheader()
        w.writerows(ctx.defeitos)
    (ctx.verdade / "resumo.json").write_text(
        json.dumps(ctx.resumo, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def fatos_para(ctx: Contexto, fonte: str) -> list[Fato]:
    return [f for f in ctx.fatos if fonte in f.fontes]
