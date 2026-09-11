#!/usr/bin/env python3
"""
gerar_intel.py — a fonte INTEL: o informe de inteligencia, PDF assinado e digitalizado.

Cabecalho fixo (Nr, grupo data-hora, origem, destinatario, assunto, avaliacao
da fonte/informacao como 'B2') e corpo em prosa numerada, como um informe
real. E o documento que CARREGA a avaliacao: a letra (A-F) e a confiabilidade
da fonte, o algarismo (1-6) a credibilidade da informacao — o de/para le do
cabecalho (split_escala), enquanto no relato do C2_B ela e inferida.

O corpo cita um lugar do gazetteer por extenso ('Ponte sobre o Corrego
Aroeira'); o modelo extrai a referencia e REF_GAZETTEER a resolve em ponto.

Sidecar: operacao, origem (secao de inteligencia), data-hora, Nr.
"""

from datetime import timedelta

from cenario import fatos_para
from comum import Contexto, assinar, dtg, escanear, escrever_sidecar, fonte, iso, local, pagina, salvar_pdf, texto_centrado

AVALIACAO = [("A", 2), ("B", 5), ("C", 4), ("D", 2), ("E", 1), ("F", 1)]
CREDIB = [("1", 2), ("2", 5), ("3", 4), ("4", 2), ("5", 1), ("6", 1)]
FONTES_TXT = {
    "A": "fonte de confiabilidade comprovada (patrulha propria)",
    "B": "fonte geralmente confiavel (observador da fracao)",
    "C": "fonte razoavelmente confiavel (morador local)",
    "D": "fonte nao usualmente confiavel",
    "E": "fonte nao confiavel",
    "F": "fonte cuja confiabilidade nao pode ser julgada",
}
CREDIB_TXT = {
    "1": "confirmada por outra fonte independente", "2": "provavelmente verdadeira",
    "3": "possivelmente verdadeira", "4": "duvidosa", "5": "improvavel", "6": "de veracidade nao julgavel",
}
ASSUNTOS_FUNDO = [
    ("Atividade de reconhecimento da forca oponente", "AVISTAMENTO_INIMIGO",
     "elementos de reconhecimento da forca oponente observados em deslocamento a pe"),
    ("Movimento de viaturas nao identificadas", "AVISTAMENTO", "viaturas civis em movimento incomum durante a noite"),
    ("Possivel posto de observacao inimigo", "AVISTAMENTO_INIMIGO", "reflexos e movimento em elevacao compativeis com posto de observacao"),
    ("Sobrevoo de aeronave remotamente pilotada", "AVISTAMENTO", "aeronave remotamente pilotada de pequeno porte sobrevoando a area"),
    ("Atividade logistica da forca oponente", "AVISTAMENTO_INIMIGO", "comboio de abastecimento da forca oponente"),
    ("Interdicao de via por fogos", "BOMBARDEIO_INIMIGO", "impactos de artilharia da forca oponente sobre a via"),
]
RECOMENDACAO = {
    "URGENTE": "Recomenda-se acao imediata e difusao urgente as fracoes desdobradas.",
    "PRIORITARIO": "Recomenda-se intensificar a vigilancia no setor e confirmar por meios proprios.",
    "ROTINA": "Para conhecimento; sem acao recomendada no momento.",
}


def _quebrar(d, txt, f, largura):
    linhas, atual = [], ""
    for palavra in txt.split():
        teste = (atual + " " + palavra).strip()
        if d.textlength(teste, font=f) <= largura:
            atual = teste
        else:
            linhas.append(atual)
            atual = palavra
    if atual:
        linhas.append(atual)
    return linhas


def _sortear(rng, pares):
    total = sum(p for _, p in pares)
    r, acc = rng.uniform(0, total), 0
    for v, p in pares:
        acc += p
        if r <= acc:
            return v
    return pares[-1][0]


def _pagina(ctx: Contexto, nr, t, origem, assunto, aval, paragrafos):
    rng = ctx.rng
    img, d = pagina()
    f_h, f_b = fonte(24), fonte(24)
    texto_centrado(d, 90, "EXERCÍCIO — OPERAÇÃO PERSEU 2024", fonte(24, negrito=True))
    texto_centrado(d, 150, f"INFORME Nr {nr:03d}/24", fonte(38, negrito=True))
    y = 260
    for rot, val in [("De:", origem), ("Para:", "Cmt 51ª Bda Inf Mtz"), ("Data-hora:", dtg(t)),
                     ("Assunto:", assunto), ("Avaliação da fonte / informação:", aval)]:
        d.text((90, y), rot, font=fonte(24, negrito=True), fill="black")
        d.text((560, y), val, font=f_h, fill="black")
        y += 52
    d.line([90, y + 10, 1564, y + 10], fill="black", width=2)
    y += 50
    for i, p in enumerate(paragrafos, start=1):
        for k, l in enumerate(_quebrar(d, f"{i}. {p}", f_b, 1380)):
            d.text((120 if k == 0 else 160, y), l, font=f_b, fill="black")
            y += 36
        y += 18
    y += 60
    assinar(d, 960, y, rng)
    d.text((900, y + 50), origem, font=f_h, fill="black")
    d.text((90, 2260), "Dado sintético — exercício de adestramento", font=fonte(16), fill="black")
    return [escanear(img, rng, ctx.np_rng)]


def gerar(ctx: Contexto) -> dict:
    rng = ctx.rng
    pasta = ctx.pasta("intel")
    itens = []
    for f in fatos_para(ctx, "INTEL"):
        assunto = {"AVISTAMENTO_INIMIGO": "Atividade da forca oponente", "BOMBARDEIO_INIMIGO": "Fogos da forca oponente",
                   }.get(f.tipo_cod, "Ocorrencia no setor")
        itens.append((f.hora_dt + timedelta(hours=rng.uniform(0.5, 4)), f.om_cod, assunto, f.descricao, f.local_nome,
                      f.hora_dt, f.prioridade_cod))
    for i in range(ctx.n(30) - len(itens)):
        assunto, _, desc = rng.choice(ASSUNTOS_FUNDO)
        om = rng.choice(ctx.por_escalao("OM"))
        lugar = rng.choice(ctx.locais(om["UNIDADE_COD"]) or ctx.gazetteer)
        t0 = local(rng.choice(ctx.operacao.dias), rng.randint(4, 22), rng.randint(0, 59))
        itens.append((t0 + timedelta(hours=rng.uniform(0.5, 5)), om["UNIDADE_COD"], assunto, desc, lugar["LOCAL_NOME"], t0,
                      rng.choice(["ROTINA", "PRIORITARIO", "PRIORITARIO", "URGENTE"])))
    itens.sort(key=lambda r: r[0])
    for nr, (t, om_cod, assunto, desc, lugar, t0, prio) in enumerate(itens, start=1):
        origem_cod = ctx.operacao.unidade_resp if rng.random() < 0.4 else om_cod
        origem = f"{'E-2' if origem_cod == ctx.operacao.unidade_resp else 'S-2'} / {ctx.sigla(origem_cod)}"
        letra, num = _sortear(rng, AVALIACAO), _sortear(rng, CREDIB)
        hora_txt = t0.astimezone(local(ctx.operacao.dias[0], 0).tzinfo).strftime("%Hh%M de %d %b").upper()
        paragrafos = [
            f"As {hora_txt}, {FONTES_TXT[letra]} informou {desc} nas proximidades de {lugar}.",
            f"A informacao e avaliada como {CREDIB_TXT[num]}. " + rng.choice([
                "Nao ha, ate o momento, contato direto com os elementos citados.",
                "Elementos da fracao no setor mantem observacao sobre a area.",
                "Solicitada confirmacao por patrulha no proximo periodo.",
            ]),
            rng.choice([
                "Avalia-se que a atividade esta relacionada a acoes de reconhecimento da forca oponente sobre o eixo da BR-154.",
                "Avalia-se que se trata de atividade isolada, sem indicio de acao coordenada.",
                "Avalia-se possivel preparacao de acao da forca oponente contra elementos desdobrados no setor.",
            ]),
            RECOMENDACAO[prio],
        ]
        caminho = pasta / f"informe_{nr:03d}_{t.astimezone(local(ctx.operacao.dias[0], 0).tzinfo).date().isoformat()}.pdf"
        salvar_pdf(_pagina(ctx, nr, t, origem, assunto, f"{letra}{num}", paragrafos), caminho)
        escrever_sidecar(caminho, {"operacao": ctx.operacao.cod, "origem": ctx.sigla(origem_cod),
                                   "data_hora": iso(t), "nr": f"{nr:03d}/24"})
    r = {"informes": len(itens)}
    ctx.resumo["INTEL"] = r
    return r


if __name__ == "__main__":
    from gerar_tudo import executar_um
    executar_um("INTEL", gerar)
