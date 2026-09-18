#!/usr/bin/env python3
"""
gerar_voz.py — a fonte VOZ: mensagens de radio em .wav, com sidecar do gravador.

A cadeia: texto da mensagem -> sintese de voz (Piper, pt-BR, CPU) -> degradacao
de radio (banda 300-3400 Hz, ruido, leve saturacao, squelch) -> .wav.

A fala e normal, em portugues, com os CODINOMES no lugar dos termos sensiveis:
  cavalo = forca oponente     potro = avistamento     brasa = incidente
  bigorna = obstaculo         martelo = missao de tiro    tambor = bombardeio inimigo
  tordilho = relato de posicao
  relampago / chuva / orvalho = urgente / prioritario / rotina
Os codinomes sao SINONIMOS no dominio tipo_evento e prioridade do modelo
canonico: 'cavalo' na transcricao casa direto com AVISTAMENTO_INIMIGO.

O sidecar e o que o gravador (voice logger) sabe: operacao, hora da
transmissao e a estacao (a unidade dona do radio, pela sigla). Tres campos,
de proposito. A hora vale como hora do fato.

A voz do Piper e baixada uma vez para <saida>/vozes/ (~60 MB).

VOZ E VOCABULARIO (achado de 2026-09-10, testado com faster-whisper small int8):
  - a voz pt_BR-cadu-medium e bem transcrita; a pt_BR-faber-medium troca palavras
    ate em audio limpo ('cavalo' -> 'tava alo'). Fica so a cadu, com a velocidade
    variando por estacao para dar variedade de locutor.
  - os codinomes sao palavras raras: sem ajuda, o transcritor escreve 'todilo'
    para 'tordilho'. Com a lista de codinomes e lugares no `initial_prompt` do
    Whisper, a transcricao sai correta. Isso e configuracao da EXTRACAO, e fica
    registrado em EXTRACAO.PROMPT_VERSAO_COD como qualquer prompt.
"""

import subprocess
import sys
import wave
from datetime import timedelta
from pathlib import Path

import numpy as np

from cenario import fatos_para
from comum import Contexto, escrever_sidecar, iso, local

VOZES = ["pt_BR-cadu-medium"]
CODINOME_TIPO = {"AVISTAMENTO_INIMIGO": "cavalo", "AVISTAMENTO": "potro", "INCIDENTE": "brasa", "OBSTACULO": "bigorna",
                 "MISSAO_TIRO": "martelo", "BOMBARDEIO_INIMIGO": "tambor", "POSICAO": "tordilho"}
CODINOME_PRIO = {"URGENTE": "relâmpago", "PRIORITARIO": "chuva", "ROTINA": "orvalho"}
CODINOME_FUNCAO = {"MOV_MANOBRA": "bússola", "INTELIGENCIA": "luneta", "FOGOS": "lança", "PROTECAO": "escudo", "LOGISTICA": "celeiro"}
INDICATIVOS = ["Trovão", "Falcão", "Jaguar", "Lobo", "Águia", "Tigre", "Cobra", "Pantera", "Condor", "Touro"]
ACENTOS = [("Corrego", "Córrego"), ("Goncalo", "Gonçalo"), ("Nucleo", "Núcleo"), ("Jatoba", "Jatobá"), ("Copaiba", "Copaíba"),
           ("Macauba", "Macaúba"), ("Pequi", "Pequi"), ("Baru", "Barú"), ("Pau-Terra", "Pau Terra"), ("km", "quilômetro")]
CORPOS = {
    "AVISTAMENTO_INIMIGO": ["Cavalo passando próximo a {lugar}, sentido {dir}.", "{n} cavalos observados em {lugar}.",
                            "Cavalo parado em {lugar}, em observação."],
    "AVISTAMENTO": ["Potro em {lugar}: {desc}.", "Potro observado próximo a {lugar}."],
    "INCIDENTE": ["Brasa em {lugar}: {desc}.", "Brasa na altura de {lugar}, {desc}."],
    "OBSTACULO": ["Bigorna em {lugar}: {desc}.", "Bigorna confirmada próximo a {lugar}."],
    "BOMBARDEIO_INIMIGO": ["Tambor sobre {lugar}, {n} impactos.", "Tambor em {lugar}, iniciado há {n} minutos."],
    "MISSAO_TIRO": ["Martelo solicitado sobre {lugar}.", "Solicito martelo em {lugar}, alvo observado."],
    "POSICAO": ["Tordilho em {lugar}. Nada a relatar.", "Tordilho: {lugar}, deslocamento normal.", "Tordilho em {lugar}, câmbio."],
}


def _baixar_vozes(dir_vozes: Path):
    dir_vozes.mkdir(parents=True, exist_ok=True)
    faltam = [v for v in VOZES if not (dir_vozes / f"{v}.onnx").exists()]
    if faltam:
        print(f"  baixando vozes do Piper: {', '.join(faltam)}")
        subprocess.run([sys.executable, "-m", "piper.download_voices", "--data-dir", str(dir_vozes)] + faltam, check=True)


def _acentuar(txt: str) -> str:
    for a, b in ACENTOS:
        txt = txt.replace(a, b)
    return txt


def _degradar_radio(ctx: Contexto, amostras: np.ndarray, taxa: int) -> np.ndarray:
    """Banda de voz estreita, ruido, saturacao e squelch — o que um radio VHF faz com a voz."""
    rng = ctx.rng
    x = amostras.astype(np.float32) / 32768.0
    esp = np.fft.rfft(x)
    freqs = np.fft.rfftfreq(len(x), 1 / taxa)
    esp[(freqs < 300) | (freqs > 3400)] = 0
    x = np.fft.irfft(esp, n=len(x))
    x = np.tanh(x * rng.uniform(1.0, 1.5)) * 0.8                     # saturacao leve
    snr_db = rng.uniform(18, 32)
    pot = np.mean(x ** 2) + 1e-9
    ruido = ctx.np_rng.normal(0, np.sqrt(pot / (10 ** (snr_db / 10))), len(x))
    x = x + ruido
    burst = int(0.08 * taxa)                                           # squelch no inicio e no fim
    x = np.concatenate([ctx.np_rng.normal(0, 0.15, burst), x, ctx.np_rng.normal(0, 0.15, burst)])
    x = x / (np.max(np.abs(x)) + 1e-6) * 0.9
    return (x * 32767).astype(np.int16)


def gerar(ctx: Contexto) -> dict:
    from piper import PiperVoice
    rng = ctx.rng
    dir_vozes = ctx.saida / "vozes"
    _baixar_vozes(dir_vozes)
    vozes = [PiperVoice.load(str(dir_vozes / f"{v}.onnx")) for v in VOZES]
    try:
        from piper import SynthesisConfig
    except ImportError:                       # versoes antigas do piper: sem variacao de velocidade
        SynthesisConfig = None
    pasta = ctx.pasta("voz")
    fracoes = ctx.por_escalao("FR")
    indicativo = {u["UNIDADE_COD"]: f"{INDICATIVOS[i % len(INDICATIVOS)]} {i // len(INDICATIVOS) + 1}" for i, u in enumerate(fracoes)}
    voz_da = {u["UNIDADE_COD"]: i % len(vozes) for i, u in enumerate(fracoes)}
    ritmo_da = {u["UNIDADE_COD"]: rng.uniform(0.9, 1.15) for u in fracoes}    # cada estacao fala num ritmo

    mensagens = []
    for f in fatos_para(ctx, "VOZ"):
        ucod = f.unidade_cod if f.unidade_cod in indicativo else rng.choice(fracoes)["UNIDADE_COD"]
        corpo = rng.choice(CORPOS[f.tipo_cod]).format(lugar=f.local_nome, dir=rng.choice(["norte", "sul", "leste", "oeste"]),
                                                       n=rng.randint(2, 9), desc=f.descricao)
        partes = [corpo, CODINOME_PRIO[f.prioridade_cod].capitalize() + "."]
        if rng.random() < 0.5:
            partes.append(CODINOME_FUNCAO[f.funcao_cod].capitalize() + ".")
        mensagens.append((f.hora_dt + timedelta(minutes=rng.randint(0, 6)), ucod, " ".join(partes)))
    for i in range(ctx.n(150) - len(mensagens)):
        u = rng.choice(fracoes)
        lugar = rng.choice(ctx.locais(ctx.om_de(u["UNIDADE_COD"])) or ctx.gazetteer)
        t = local(rng.choice(ctx.operacao.dias), rng.randint(5, 23), rng.randint(0, 59), rng.randint(0, 59))
        mensagens.append((t, u["UNIDADE_COD"], rng.choice(CORPOS["POSICAO"]).format(lugar=lugar["LOCAL_NOME"]) + " Orvalho."))
    mensagens.sort(key=lambda m: m[0])

    total_s = 0.0
    for i, (t, ucod, corpo) in enumerate(mensagens, start=1):
        texto = _acentuar(f"Centro de operações, aqui {indicativo[ucod]}. {corpo} Câmbio.")
        voz = vozes[voz_da[ucod]]
        caminho = pasta / f"msg_{i:04d}_{t.astimezone(local(ctx.operacao.dias[0], 0).tzinfo).strftime('%Y%m%d_%H%M')}.wav"
        with wave.open(str(caminho), "wb") as w:
            if SynthesisConfig:
                voz.synthesize_wav(texto, w, syn_config=SynthesisConfig(length_scale=ritmo_da[ucod]))
            else:
                voz.synthesize_wav(texto, w)
        with wave.open(str(caminho), "rb") as w:
            taxa, n = w.getframerate(), w.getnframes()
            amostras = np.frombuffer(w.readframes(n), dtype=np.int16)
        degradado = _degradar_radio(ctx, amostras, taxa)
        with wave.open(str(caminho), "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(taxa)
            w.writeframes(degradado.tobytes())
        dur = round(len(degradado) / taxa, 2)
        total_s += dur
        escrever_sidecar(caminho, {"operacao": ctx.operacao.cod, "hora_transmissao": iso(t), "estacao": ctx.sigla(ucod), "duracao_s": dur})
        # A frase EXATA que foi sintetizada. Sem ela nao ha como medir o acerto da
        # transcricao — o audio e a unica testemunha, e ele nao se explica.
        ctx.registrar_gabarito("VOZ", caminho.relative_to(ctx.saida), "texto", texto)
    r = {"mensagens": len(mensagens), "audio_segundos": round(total_s)}
    ctx.resumo["VOZ"] = r
    return r


if __name__ == "__main__":
    from gerar_tudo import executar_um
    executar_um("VOZ", gerar)
