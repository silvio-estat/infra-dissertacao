#!/usr/bin/env python3
"""
gerar_c2b.py — a fonte C2_B: relato em texto livre e incidente com foto.

  relato/      um JSON por relato, como o aplicativo exporta: data no formato
               brasileiro, relator ('<posto> <funcao> <fracao> - <IDENTIDADE_MIL>'),
               localizacao em grau-minuto-segundo (ou 'Sem localizacao') e a
               narrativa livre. A narrativa e a ENTRADA do modelo de linguagem:
               tipo, funcao de combate, prioridade e confiabilidade nao vem em
               campo — o modelo deduz do texto, e o estado-maior confere.
  incidente/   um JSON + um JPG de mesmo nome. O JSON e o sidecar da foto
               (operacao, unidade, hora). A foto e uma cena sintetica com
               EXIF (GPS + data) — a coordenada do evento vem DO EXIF, nao do
               que o operador digitou, e as duas podem divergir.

Defeitos plantados (ver verdade/defeitos.csv):
  sem_exif          foto encaminhada por app de mensagem perdeu o metadado
  exif_divergente   o GPS da foto nao bate com a coordenada digitada
  foto_reenviada    a MESMA foto anexada a dois incidentes de fracoes diferentes
"""

import io
import random
import shutil
from datetime import timedelta

import numpy as np
import piexif
from PIL import Image, ImageDraw, ImageFilter

from cenario import fatos_para
from comum import Contexto, br, deslocar, dms, escrever_json, fonte, jitter, local

POSTOS = ["Cap", "1o Ten", "2o Ten", "Asp", "1o Sgt", "2o Sgt", "3o Sgt", "Cb"]
FUNCOES = ["Cmt", "SCmt", "Adj", "Obs", "Ch Pel", "Rd Op"]

# frases que dao ao modelo pistas de PRIORIDADE (sem dizer o codigo)
PRIORIDADE_TXT = {
    "URGENTE": ["Solicita apoio imediato.", "Situacao critica, necessita reforco urgente.", "Acao imediata em curso."],
    "PRIORITARIO": ["Acompanhando a situacao.", "Solicita orientacao do COp.", "Mantendo observacao."],
    "ROTINA": ["Sem necessidade de apoio.", "Nada mais a relatar.", "Situacao sob controle."],
}
# pistas de CONFIABILIDADE da fonte / CREDIBILIDADE da informacao (escala A-F / 1-6)
CONFIAB_TXT = [
    "Informacao confirmada por dois observadores da fracao.",           # ~A1
    "Observado diretamente pela patrulha.",                             # ~B2
    "Informado por civil da localidade, ainda nao confirmado.",         # ~C3-4
    "Ouvido em conversa de moradores, sem confirmacao.",                # ~D5
    "Origem da informacao nao pode ser avaliada.",                      # ~F6
]
ROTINA = [
    "Patrulha no eixo {rodovia} realizada sem alteracao.",
    "Fracao instalada em {lugar}, PC operando normalmente.",
    "Reabastecimento de Cl III concluido em {lugar}.",
    "Posto de bloqueio em {lugar} montado e operando.",
    "Deslocamento para {lugar} concluido, efetivo completo.",
    "Contato com lideranca local em {lugar}, sem ocorrencias.",
    "Revezamento de guarda realizado, nada a relatar.",
]
ABREV = [("viatura", "Vtr"), ("viaturas", "Vtr"), ("aproximadamente", "aprox"), ("companhia", "Cia"),
         ("pelotao", "Pel"), ("posicao", "Pos"), ("municao", "Mun"), ("forca oponente", "F Op"),
         ("comandante", "Cmt"), ("reforco", "Ref"), ("observacao", "Obs")]

# incidentes de fundo (alem dos do cenario)
INCIDENTES_FUNDO = [
    ("Vtr com pneu furado no eixo", "ROTINA"), ("Militar com mal-estar, atendido no local", "PRIORITARIO"),
    ("Civil solicitando informacao no posto de bloqueio", "ROTINA"), ("Queda de arvore obstruindo a via", "PRIORITARIO"),
    ("Animal na pista, transito interrompido", "ROTINA"), ("Radio da fracao com falha intermitente", "PRIORITARIO"),
    ("Foco de incendio em vegetacao proximo ao PC", "URGENTE"), ("Comboio civil bloqueando a via", "PRIORITARIO"),
]
PRIO_ORIGEM = {"URGENTE": ["Alta", "Critica"], "PRIORITARIO": ["Media", "Moderada"], "ROTINA": ["Baixa", "Normal"]}


# =============================================================================
def _relator(ctx: Contexto, unidade_cod: str) -> str:
    rng = ctx.rng
    return f"{rng.choice(POSTOS)} {rng.choice(FUNCOES)} {ctx.sigla(unidade_cod)} - {rng.randint(10**9, 10**10 - 1)}"


def _abreviar(rng: random.Random, txt: str) -> str:
    for cheio, curto in ABREV:
        if cheio in txt and rng.random() < 0.5:
            txt = txt.replace(cheio, curto)
    if rng.random() < 0.15:
        txt = txt.lower()
    return txt


def gerar_relatos(ctx: Contexto) -> dict:
    rng = ctx.rng
    pasta = ctx.pasta("c2b", "relato")
    relatos = []
    # 1) relatos que narram fatos do cenario
    for f in fatos_para(ctx, "C2B_RELATO"):
        t = f.hora_dt + timedelta(minutes=rng.randint(3, 45))
        hora_txt = f.hora_dt.astimezone(ctx_fuso()).strftime("%H%M")
        texto = (f"As {hora_txt}, {ctx.sigla(f.unidade_cod)} informa {f.descricao} nas proximidades de "
                 f"{f.local_nome}. {rng.choice(PRIORIDADE_TXT[f.prioridade_cod])} {rng.choice(CONFIAB_TXT)}")
        lat, lon = jitter(f.lat, f.lon, 0.15, rng)
        relatos.append((t, f.unidade_cod, lat, lon, texto))
    # 2) relatos de rotina
    for i in range(ctx.n(300) - len(relatos)):
        u = rng.choice(ctx.por_escalao("FR"))
        om = ctx.om_de(u["UNIDADE_COD"])
        lugar = rng.choice(ctx.locais(om) or ctx.gazetteer)
        rodovia = rng.choice(["BR-154", "VC-231"])
        t = local(rng.choice(ctx.operacao.dias), rng.randint(5, 23), rng.randint(0, 59), rng.randint(0, 59))
        texto = rng.choice(ROTINA).format(rodovia=rodovia, lugar=lugar["LOCAL_NOME"]) + " " + rng.choice(PRIORIDADE_TXT["ROTINA"])
        lat, lon = jitter(lugar["lat"], lugar["lon"], 0.5, rng)
        relatos.append((t, u["UNIDADE_COD"], lat, lon, texto))
    relatos.sort(key=lambda r: r[0])
    for i, (t, ucod, lat, lon, texto) in enumerate(relatos, start=1):
        registro = {
            "id": 10000 + i,
            "data": br(t),
            "relator": _relator(ctx, ucod),
            "localizacao": "Sem localizacao" if rng.random() < 0.15 else dms(lat, lon),
            "situacao": _abreviar(rng, texto),
            "operacao": ctx.operacao.cod,
        }
        escrever_json(pasta / f"relato_{registro['id']}.json", registro)
    return {"registros": len(relatos), "arquivos": len(relatos)}


# =============================================================================
def _foto(ctx: Contexto, especie: str, legenda: str, noite: bool) -> Image.Image:
    """Cena sintetica: ceu, terreno, via e uma forma que lembra o objeto do incidente."""
    rng = ctx.rng
    w, h = 1280, 960
    img = Image.new("RGB", (w, h))
    d = ImageDraw.Draw(img)
    ceu = (25, 28, 45) if noite else (rng.randint(120, 160), rng.randint(160, 190), rng.randint(200, 230))
    chao = (35, 32, 25) if noite else (rng.randint(90, 120), rng.randint(90, 110), rng.randint(50, 70))
    for y in range(h):                       # gradiente ceu -> chao
        k = y / h
        cor = tuple(int(ceu[i] * (1 - k) + chao[i] * k) for i in range(3))
        d.line([(0, y), (w, y)], fill=cor)
    horizonte = int(h * rng.uniform(0.38, 0.48))
    d.rectangle([0, horizonte, w, h], fill=chao)
    # via em perspectiva
    via = (60, 60, 62) if not noite else (28, 28, 30)
    d.polygon([(w * 0.45, horizonte), (w * 0.55, horizonte), (w * 0.95, h), (w * 0.05, h)], fill=via)
    cx, cy = w // 2, int(h * 0.72)
    if especie in ("viatura", "comboio"):
        for k in range(1 if especie == "viatura" else 3):
            x = cx - 250 + k * 230
            d.rectangle([x, cy - 90, x + 200, cy + 20], fill=(70, 80, 50))
            d.rectangle([x + 30, cy - 150, x + 150, cy - 90], fill=(60, 70, 45))
            d.ellipse([x + 15, cy, x + 65, cy + 50], fill=(15, 15, 15))
            d.ellipse([x + 135, cy, x + 185, cy + 50], fill=(15, 15, 15))
    elif especie == "cratera":
        d.ellipse([cx - 220, cy - 70, cx + 220, cy + 90], fill=(30, 25, 20))
        d.ellipse([cx - 160, cy - 40, cx + 160, cy + 60], fill=(20, 16, 12))
    elif especie == "barreira":
        for k in range(4):
            d.rectangle([cx - 300, cy - 30 + k * 18, cx + 300, cy - 14 + k * 18], fill=(90, 60, 30))
    elif especie == "campo de minas":
        for k in range(6):
            x = cx - 330 + k * 130
            d.polygon([(x, cy - 60), (x + 40, cy - 60), (x + 20, cy - 110)], fill=(200, 40, 40))
            d.line([(x + 20, cy - 60), (x + 20, cy)], fill=(120, 120, 120), width=6)
    elif especie == "pessoa":
        for k in range(rng.randint(1, 4)):
            x = cx - 200 + k * 130
            d.ellipse([x - 22, cy - 190, x + 22, cy - 146], fill=(60, 50, 40))
            d.rectangle([x - 30, cy - 146, x + 30, cy - 40], fill=(55, 65, 40))
            d.rectangle([x - 26, cy - 40, x - 6, cy + 40], fill=(45, 50, 35))
            d.rectangle([x + 6, cy - 40, x + 26, cy + 40], fill=(45, 50, 35))
    else:  # generico: caixa/artefato
        d.rectangle([cx - 80, cy - 60, cx + 80, cy + 30], fill=(120, 110, 90))
    # legenda queimada, como camera de campo
    f = fonte(30, mono=True)
    d.rectangle([0, h - 52, w, h], fill=(0, 0, 0))
    d.text((16, h - 44), legenda, font=f, fill=(255, 220, 80))
    # degradacao: ruido, desfoque, baixa luz
    arr = np.asarray(img).astype(np.float32)
    arr += ctx.np_rng.normal(0, rng.uniform(4, 18), arr.shape)
    if noite:
        arr *= rng.uniform(0.35, 0.6)
    img = Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8))
    return img.filter(ImageFilter.GaussianBlur(rng.uniform(0.2, 1.4)))


def _exif(lat: float, lon: float, t) -> bytes:
    def racional(v):
        v = abs(v)
        g = int(v)
        m = int((v - g) * 60)
        s = round(((v - g) * 60 - m) * 60 * 100)
        return ((g, 1), (m, 1), (s, 100))
    gps = {
        piexif.GPSIFD.GPSLatitudeRef: b"S" if lat < 0 else b"N", piexif.GPSIFD.GPSLatitude: racional(lat),
        piexif.GPSIFD.GPSLongitudeRef: b"W" if lon < 0 else b"E", piexif.GPSIFD.GPSLongitude: racional(lon),
    }
    exif = {
        "0th": {piexif.ImageIFD.Make: b"Dispositivo movel", piexif.ImageIFD.Model: b"C2_B Movel"},
        "Exif": {piexif.ExifIFD.DateTimeOriginal: t.astimezone(ctx_fuso()).strftime("%Y:%m:%d %H:%M:%S").encode()},
        "GPS": gps,
    }
    return piexif.dump(exif)


def ctx_fuso():
    from comum import FUSO
    return FUSO


def gerar_incidentes(ctx: Contexto) -> dict:
    rng = ctx.rng
    pasta = ctx.pasta("c2b", "incidente")
    itens = []
    for f in fatos_para(ctx, "C2B_INCIDENTE"):
        especie = {"OBSTACULO": f.especie or "barreira", "INCIDENTE": rng.choice(["viatura", "pessoa", "generico"])}.get(f.tipo_cod, "generico")
        itens.append((f.hora_dt + timedelta(minutes=rng.randint(2, 30)), f.unidade_cod, f.lat, f.lon,
                      f.descricao, f.prioridade_cod, especie))
    for i in range(ctx.n(100) - len(itens)):
        u = rng.choice(ctx.por_escalao("FR"))
        lugar = rng.choice(ctx.locais(ctx.om_de(u["UNIDADE_COD"])) or ctx.gazetteer)
        lat, lon = jitter(lugar["lat"], lugar["lon"], 0.6, rng)
        desc, prio = rng.choice(INCIDENTES_FUNDO)
        t = local(rng.choice(ctx.operacao.dias), rng.randint(5, 23), rng.randint(0, 59), rng.randint(0, 59))
        itens.append((t, u["UNIDADE_COD"], lat, lon, desc, prio, rng.choice(["viatura", "comboio", "pessoa", "generico"])))
    itens.sort(key=lambda r: r[0])

    escritos, reenvio_pendente = [], []
    for i, (t, ucod, lat, lon, desc, prio, especie) in enumerate(itens, start=1):
        cod = f"INC-{i:04d}"
        base = pasta / f"incidente_{i:04d}"
        noite = t.astimezone(ctx_fuso()).hour >= 19 or t.astimezone(ctx_fuso()).hour < 6
        legenda = f"{cod}  {t.astimezone(ctx_fuso()).strftime('%d%b%y %H%M').upper()}  {ctx.sigla(ucod)[:28]}"
        img = _foto(ctx, especie, legenda, noite)
        # coordenada digitada x coordenada do EXIF
        lat_exif, lon_exif = jitter(lat, lon, 0.05, rng)
        sorteio = rng.random()
        exif_bytes = None
        if sorteio < 0.20:
            ctx.registrar_defeito("C2_B", base.with_suffix(".jpg").relative_to(ctx.saida), "sem_exif",
                                  "foto encaminhada por app de mensagem: sem GPS nem data no EXIF")
        else:
            if sorteio < 0.25:
                lat_exif, lon_exif = deslocar(lat, lon, rng.uniform(3, 8) * rng.choice((-1, 1)), rng.uniform(3, 8) * rng.choice((-1, 1)))
                ctx.registrar_defeito("C2_B", base.with_suffix(".jpg").relative_to(ctx.saida), "exif_divergente",
                                      "GPS da foto a varios km da coordenada digitada")
            exif_bytes = _exif(lat_exif, lon_exif, t)
        buf = io.BytesIO()
        if exif_bytes:
            img.save(buf, "JPEG", quality=rng.randint(35, 80), exif=exif_bytes)
        else:
            img.save(buf, "JPEG", quality=rng.randint(35, 80))
        base.with_suffix(".jpg").write_bytes(buf.getvalue())
        registro = {
            "id": cod,
            "data_hora": br(t),
            "descricao": desc,
            "prioridade": rng.choice(PRIO_ORIGEM[prio]),
            "c_op": ctx.sigla(ucod),
            "localizacao_informada": dms(lat, lon),
            "foto": base.with_suffix(".jpg").name,
            "acoes": [{"hora": br(t + timedelta(minutes=rng.randint(5, 60))), "acao": a}
                      for a in rng.sample(["Equipe deslocada ao local", "COp informado", "Area isolada",
                                           "Apoio solicitado", "Ocorrencia encerrada"], k=rng.randint(1, 3))],
            "operacao": ctx.operacao.cod,
        }
        escrever_json(base.with_suffix(".json"), registro)
        escritos.append((base, ucod, t, registro))

    # defeito: a mesma foto anexada a um segundo incidente, de outra fracao, minutos depois
    n_dup = min(3, max(1, len(escritos) // 30))
    for base, ucod, t, registro in rng.sample(escritos, k=n_dup):
        j = len(escritos) + len(reenvio_pendente) + 1
        outra = rng.choice([u for u in ctx.por_escalao("FR") if u["UNIDADE_COD"] != ucod])
        novo = pasta / f"incidente_{j:04d}"
        shutil.copyfile(base.with_suffix(".jpg"), novo.with_suffix(".jpg"))
        reg = dict(registro, id=f"INC-{j:04d}", data_hora=br(t + timedelta(minutes=rng.randint(8, 50))),
                   c_op=outra["UNIDADE_SIGLA"], foto=novo.with_suffix(".jpg").name,
                   descricao=registro["descricao"] + " (encaminhado)")
        escrever_json(novo.with_suffix(".json"), reg)
        reenvio_pendente.append(novo)
        ctx.registrar_defeito("C2_B", novo.with_suffix(".jpg").relative_to(ctx.saida), "foto_reenviada",
                              f"mesma foto de {base.name}.jpg anexada por outra fracao; um binario, dois eventos")
    n = len(escritos) + len(reenvio_pendente)
    return {"registros": n, "arquivos": 2 * n}


def gerar(ctx: Contexto) -> dict:
    r = {"relato": gerar_relatos(ctx), "incidente": gerar_incidentes(ctx)}
    ctx.resumo["C2_B"] = r
    return r


if __name__ == "__main__":
    from gerar_tudo import executar_um
    executar_um("C2_B", gerar)
