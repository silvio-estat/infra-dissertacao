#!/usr/bin/env python3
"""
gerar_fogos.py — a fonte FOGOS: o Relatorio de Bombardeio, Anexo I do EB60-ME-12.301.

FIEL ao formulario do manual: cabecalho (Rcb por / Do / Hora / Nr) e as tres
partes —
  1a  Informes de Observadores e Analise de Crateras, colunas A a L
  2a  Outras Fontes, colunas M a R
  3a  Execucao do Tiro, colunas S a V (preenchido pelo S-3)
— produzido ja como PDF digitalizado: o S-2 preenche o impresso e escaneia.

So a 1a parte vira evento no de/para (uma linha = um informe de observador
sobre bombardeio INIMIGO). A 2a e a 3a ficam no PDF, para o OCR ler e para
quem quiser ampliar o escopo. Coordenadas na forma decametrica do manual
('06700-13150'); grupo data-hora na col_A ('271430 NOV 24').

Sidecar: operacao, origem (o S-2 do GAC), data-hora e Nr do relatorio.
"""

from datetime import timedelta

from cenario import fatos_para
from comum import (Contexto, assinar, decametrica, desenhar_tabela, dtg, escanear, escrever_sidecar, fonte, hora_curta,
                   iso, jitter, local, pagina, salvar_pdf, texto_centrado)

OBSERVADORES = ["TROVAO", "CARCARA", "JACARE", "GAVIAO", "SAPO", "TATU"]
ARMAMENTO = [("Ob", "Me"), ("Ob", "L"), ("Mrt", "Me"), ("Mrt", "P"), ("Can", "Me"), ("Fgt", "P")]
CLASSIFICACAO = ["Ntz", "Dest", "Itd", "Inqt", "Regl", "Tir Esp"]
MUNICAO = ["Expl", "Fumig", "Ilm", "Expl - E Itt"]
EFEITOS = ["Sem baixas", "Sem danos observados", "1 Vtr Dest", "2 Bx, 1 Vtr Avar", "Ntz da {fr}", "3 Bx",
           "Via interditada", "Danos leves no PC", "1 Bx, abrigos danificados"]
MEIOS = ["Obs Ae", "Radar", "Obs Av", "PO", "Loc Som"]
ARTILHARIA_SIGLA = "51o GAC"
BRIGADA_SIGLA = "51a Bda Inf Mtz"


def _linha_parte1(ctx: Contexto, t, lat, lon, fracao_sigla, prioridade):
    rng = ctx.rng
    obs = f"{rng.choice(OBSERVADORES)} {rng.randint(1, 4)}"
    lat_o, lon_o = jitter(lat, lon, 2.5, rng)                  # o observador esta a alguns km
    ini = t - timedelta(minutes=rng.randint(5, 40))
    fim = ini + timedelta(minutes=rng.randint(1, 14))
    q, (tipo, cal) = rng.randint(1, 4), rng.choice(ARMAMENTO)
    efeito = rng.choice(EFEITOS[3:] if prioridade == "URGENTE" else EFEITOS[:3]).format(fr=fracao_sigla.split("/")[0])
    return [f"{obs}\n{dtg(t)}", decametrica(lat_o, lon_o), f"{rng.randint(800, 6300)} mil", hora_curta(ini), hora_curta(fim),
            decametrica(lat, lon), f"{q}/{tipo}/{cal}", rng.choice(CLASSIFICACAO), f"{rng.randint(4, 40)}/ {rng.choice(MUNICAO)}",
            str(rng.randint(3, 15)), efeito]


def _linha_parte2(ctx: Contexto, t, lat, lon):
    rng = ctx.rng
    lat2, lon2 = jitter(lat, lon, 4, rng)
    return [f"{rng.choice(OBSERVADORES)} {rng.randint(1, 4)}\n{dtg(t + timedelta(minutes=rng.randint(5, 90)))}",
            f"{decametrica(lat2, lon2)}\n{rng.choice([30, 50, 100, 200])} m", rng.choice(MEIOS), hora_curta(t),
            f"{rng.randint(1, 4)}/{rng.choice(['?', 'Ob', 'Mrt'])}", rng.choice(["Quatro Pecas", "Duas Pecas", "Em espaldao", "Linha de Vtr"])]


def _desenhar(ctx: Contexto, nr, t, p1, p2, p3) -> list:
    rng = ctx.rng
    img, d = pagina()
    f_t, f_h, f_c, f_x = fonte(36, negrito=True), fonte(21), fonte(15, negrito=True), fonte(16)
    texto_centrado(d, 70, "RELATÓRIO DE BOMBARDEIO", f_t)
    d.text((60, 140), f"Rcb por: E-2 da {BRIGADA_SIGLA}", font=f_h, fill="black")
    d.text((760, 140), f"Do: S-2 do {ARTILHARIA_SIGLA}", font=f_h, fill="black")
    d.text((1250, 140), f"Hora: {hora_curta(t)}", font=f_h, fill="black")
    d.text((1480, 140), f"Nr: {nr:02d}", font=f_h, fill="black")
    texto_centrado(d, 200, "1ª PARTE — INFORMES DE OBSERVADORES E ANÁLISE DE CRATERAS", fonte(22, negrito=True))
    cab1 = ["A\nInforme de\nHora (1)", "B\nLocalização do\nObservador (2)", "C\nLançamento do\nSom ou Cratera (3)",
            "D\nHora Ativ\nDe (4)", "E\n\nÀs", "F\nÁrea\nBombardeada (5)", "G\nQnt e Tipo de\nArmamento Ini (6)",
            "H\nClassificação do\nTiro Ini (7)", "I\nQnt e Tipo de\nMunição Ini (8)", "J\nClarão-Som\nsegundos", "L\nEfeito dos\nFogos Ini (9)"]
    larg1 = [190, 140, 130, 90, 80, 140, 150, 140, 150, 100, 264]
    y = _tabela_multilinha(d, 40, 250, larg1, cab1, p1, f_c, f_x)
    texto_centrado(d, y + 40, "2ª PARTE — OUTRAS FONTES", fonte(22, negrito=True))
    cab2 = ["M\nInforme de\nHora (10)", "N\nCoordenadas do Alvo\nPrecisão (11)", "O\nMeio\nUtilizado (12)",
            "P\nHora da\nAtividade (13)", "Q\nQnt e Tipo do\nArmamento (14)", "R\nObs (15)"]
    larg2 = [260, 300, 220, 200, 260, 334]
    y = _tabela_multilinha(d, 40, y + 90, larg2, cab2, p2 or [["-x-"] * 6], f_c, f_x)
    texto_centrado(d, y + 40, "3ª PARTE — EXECUÇÃO DO TIRO (16) — PREENCHIDO PELO S-3", fonte(22, negrito=True))
    cab3 = ["S\nMiss", "T\nExec por", "U\nMun Ut", "V\nEfeito"]
    y = _tabela_multilinha(d, 40, y + 90, [200, 400, 450, 524], cab3, p3, f_c, f_x)
    assinar(d, 1000, y + 120, rng)
    d.text((1000, y + 170), f"S-2 / {ARTILHARIA_SIGLA}", font=f_h, fill="black")
    d.text((60, 2260), "EXERCÍCIO — Operação Perseu 2024 — dado sintético", font=fonte(16), fill="black")
    return [escanear(img, rng, ctx.np_rng)]


def _tabela_multilinha(d, x0, y0, larguras, cabecalho, linhas, f_cab, f_txt):
    """Como desenhar_tabela, mas celulas com quebra de linha ('\\n'), como o formulario impresso."""
    def bloco(y, textos, f, alt):
        d.rectangle([x0, y, x0 + sum(larguras), y + alt], outline="black", width=2)
        x = x0
        for w, txt in zip(larguras, textos):
            d.line([x, y, x, y + alt], fill="black", width=2)
            for k, parte in enumerate(str(txt).split("\n")):
                while parte and d.textlength(parte, font=f) > w - 10:
                    parte = parte[:-1]
                d.text((x + 6, y + 8 + k * (f.size + 6)), parte, font=f, fill="black")
            x += w
        return y + alt
    y = bloco(y0, cabecalho, f_cab, 3 * (f_cab.size + 6) + 16)
    for l in linhas:
        n = max(str(c).count("\n") + 1 for c in l)
        y = bloco(y, l, f_txt, n * (f_txt.size + 6) + 16)
    return y


def gerar(ctx: Contexto) -> dict:
    rng = ctx.rng
    pasta = ctx.pasta("fogos")
    # agrupa os fatos de bombardeio por dia; cada relatorio consolida os informes do dia
    por_dia: dict = {}
    for f in fatos_para(ctx, "FOGOS"):
        por_dia.setdefault(f.hora_dt.astimezone(local(ctx.operacao.dias[0], 0).tzinfo).date(), []).append(f)
    n_relatorios = ctx.n(30)
    dias = list(ctx.operacao.dias)
    relatorios = []
    for dia, fatos in por_dia.items():
        relatorios.append((dia, fatos))
    while len(relatorios) < n_relatorios:
        relatorios.append((rng.choice(dias), []))
    relatorios.sort(key=lambda r: r[0])
    fr_inf = [u for u in ctx.por_escalao("FR") if u["UNIDADE_COD"].startswith("BI")]
    total_linhas = 0
    for nr, (dia, fatos) in enumerate(relatorios, start=1):
        t = local(dia, rng.randint(8, 22), rng.randint(0, 59))
        p1 = []
        for f in fatos:
            p1.append(_linha_parte1(ctx, f.hora_dt, f.lat, f.lon, ctx.sigla(f.unidade_cod), f.prioridade_cod))
        for _ in range(rng.randint(max(0, 2 - len(p1)), max(0, 4 - len(p1)))):   # 2 a 4 informes por relatorio
            u = rng.choice(fr_inf)
            lugar = rng.choice(ctx.locais(ctx.om_de(u["UNIDADE_COD"])) or ctx.gazetteer)
            lat, lon = jitter(lugar["lat"], lugar["lon"], 1.0, rng)
            p1.append(_linha_parte1(ctx, t - timedelta(hours=rng.randint(1, 9)), lat, lon, u["UNIDADE_SIGLA"],
                                    rng.choice(["ROTINA", "PRIORITARIO", "URGENTE"])))
        p2 = []
        for _ in range(rng.randint(0, 2)):
            lugar = rng.choice(ctx.gazetteer)
            lat, lon = jitter(lugar["lat"], lugar["lon"], 1.0, rng)
            p2.append(_linha_parte2(ctx, t - timedelta(hours=rng.randint(1, 6)), lat, lon))
        p3 = [[f"{rng.randint(1, 40):03d}", ARTILHARIA_SIGLA, f"Q{rng.randint(1, 6)} - {rng.choice(MUNICAO)}",
               rng.choice(["Pecas de Mrt Ini Dest", "Bateria Ini Ntz", "Alvo Ntz 60%", "Sem efeito observado"])]]
        caminho = pasta / f"relatorio_bombardeio_{nr:03d}_{dia.isoformat()}.pdf"
        salvar_pdf(_desenhar(ctx, nr, t, p1, p2, p3), caminho)
        escrever_sidecar(caminho, {"operacao": ctx.operacao.cod, "origem": ARTILHARIA_SIGLA, "data_hora": iso(t), "nr": f"RB Nr {nr:02d}"})
        total_linhas += len(p1)
    r = {"relatorios": len(relatorios), "informes_1a_parte": total_linhas}
    ctx.resumo["FOGOS"] = r
    return r


if __name__ == "__main__":
    from gerar_tudo import executar_um
    executar_um("FOGOS", gerar)
