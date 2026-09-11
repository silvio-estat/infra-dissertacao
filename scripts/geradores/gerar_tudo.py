#!/usr/bin/env python3
"""
gerar_tudo.py — gera o conjunto sintetico completo da Operacao Perseu 2024.

    source venv-geradores/bin/activate
    python scripts/geradores/gerar_tudo.py                 # tudo, volumes padrao
    python scripts/geradores/gerar_tudo.py --escala 0.1    # teste rapido
    python scripts/geradores/gerar_tudo.py --sem-voz       # pula o audio (o mais lento)
    python scripts/geradores/gerar_tudo.py --fontes C2_A RELPER

Saida (gitignored):
    dados_sinteticos/landing/perseu_2024/<fonte>/...   o que vai para o MinIO
    dados_sinteticos/verdade/fatos.csv                 os fatos do cenario (inspecao; o pipeline nao le)
    dados_sinteticos/verdade/defeitos.csv              os defeitos plantados de proposito
    dados_sinteticos/verdade/resumo.json               contagens por fonte

Depois: python scripts/geradores/enviar_landing.py

A mesma semente produz os mesmos arquivos (ids, sorteios, ruido). Rodar um
gerador sozinho (python gerar_c2b.py) usa a mesma semente, mas a sequencia de
sorteios e outra: os arquivos nao coincidem byte a byte com os da rodada completa.
"""

import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cenario import gerar_cenario, salvar_verdade   # noqa: E402
from comum import SAIDA_PADRAO, Contexto            # noqa: E402

ORDEM = ["C2_A", "C2_B", "RELPER", "FOGOS", "INTEL", "VOZ"]


def _geradores():
    import gerar_c2a, gerar_c2b, gerar_fogos, gerar_intel, gerar_relper, gerar_voz
    return {"C2_A": gerar_c2a.gerar, "C2_B": gerar_c2b.gerar, "RELPER": gerar_relper.gerar,
            "FOGOS": gerar_fogos.gerar, "INTEL": gerar_intel.gerar, "VOZ": gerar_voz.gerar}


def _args(argv=None):
    p = argparse.ArgumentParser(description="Gera os dados sinteticos multimodais da Operacao Perseu 2024.")
    p.add_argument("--seed", type=int, default=2024)
    p.add_argument("--saida", type=Path, default=SAIDA_PADRAO)
    p.add_argument("--escala", type=float, default=1.0, help="multiplica os volumes (0.1 = teste rapido)")
    p.add_argument("--fontes", nargs="+", choices=ORDEM, default=ORDEM)
    p.add_argument("--sem-voz", action="store_true", help="nao gera o audio")
    p.add_argument("--manter", action="store_true", help="nao apaga a saida anterior")
    return p.parse_args(argv)


def _preparar(ctx: Contexto, manter: bool):
    landing = ctx.saida / "landing"
    if landing.exists() and not manter:
        shutil.rmtree(landing)
    if ctx.verdade.exists() and not manter:
        shutil.rmtree(ctx.verdade)


def _tamanho_mb(pasta: Path) -> float:
    return sum(p.stat().st_size for p in pasta.rglob("*") if p.is_file()) / 1e6 if pasta.exists() else 0.0


def executar(args) -> Contexto:
    ctx = Contexto(seed=args.seed, saida=args.saida, escala=args.escala)
    _preparar(ctx, args.manter)
    print(f"Operacao {ctx.operacao.cod}: {ctx.operacao.dias[0]} a {ctx.operacao.dias[-1]} "
          f"({len(ctx.operacao.dias)} dias) | semente {args.seed} | escala {args.escala}")
    fatos = gerar_cenario(ctx)
    print(f"Cenario: {len(fatos)} fatos")
    fontes = [f for f in args.fontes if not (args.sem_voz and f == "VOZ")]
    geradores = _geradores()
    for nome in fontes:
        t0 = time.time()
        r = geradores[nome](ctx)
        print(f"  {nome:7s} {time.time() - t0:6.1f}s  {r}")
    salvar_verdade(ctx)
    print("\nResumo por pasta (MB):")
    for pasta in sorted(p for p in ctx.landing.iterdir() if p.is_dir()):
        arquivos = sum(1 for p in pasta.rglob("*") if p.is_file())
        print(f"  {pasta.name:10s} {arquivos:6d} arquivos  {_tamanho_mb(pasta):8.1f} MB")
    print(f"\n{len(ctx.defeitos)} defeitos plantados -> {ctx.verdade / 'defeitos.csv'}")
    print(f"landing em {ctx.landing}")
    return ctx


def executar_um(nome: str, fn):
    """Ponto de entrada de cada gerador rodado sozinho."""
    args = _args()
    args.fontes = [nome]
    args.manter = True
    ctx = Contexto(seed=args.seed, saida=args.saida, escala=args.escala)
    gerar_cenario(ctx)
    r = fn(ctx)
    salvar_verdade(ctx)
    print(f"{nome}: {r}")


if __name__ == "__main__":
    executar(_args())
