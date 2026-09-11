#!/usr/bin/env python3
"""
enviar_landing.py — copia dados_sinteticos/landing/ para o MinIO, em s3://lakehouse/landing/.

Roda na maquina host (fora do Docker), por isso fala com localhost:9000.
Le as credenciais do .env (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).

    python scripts/geradores/enviar_landing.py             # envia o que houver
    python scripts/geradores/enviar_landing.py --limpar    # apaga landing/ no bucket antes
    python scripts/geradores/enviar_landing.py --so-listar # mostra o que seria enviado

O par binario + sidecar sobe junto, no mesmo prefixo, com o mesmo nome:
    landing/perseu_2024/relper/relper_bi511_2024-11-27_matutino.xlsx
    landing/perseu_2024/relper/relper_bi511_2024-11-27_matutino.json
"""

import argparse
import mimetypes
import os
from pathlib import Path

from minio import Minio

RAIZ = Path(__file__).resolve().parents[2]


def _env():
    env = {}
    caminho = RAIZ / ".env"
    if caminho.exists():
        for linha in caminho.read_text(encoding="utf-8").splitlines():
            if "=" in linha and not linha.strip().startswith("#"):
                k, v = linha.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    env.update({k: v for k, v in os.environ.items() if k.startswith("MINIO_")})
    return env


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--saida", type=Path, default=RAIZ / "dados_sinteticos")
    p.add_argument("--endpoint", default="localhost:9000")
    p.add_argument("--bucket", default=None)
    p.add_argument("--limpar", action="store_true")
    p.add_argument("--so-listar", action="store_true")
    args = p.parse_args()

    env = _env()
    bucket = args.bucket or env.get("MINIO_BUCKET_LAKEHOUSE", "lakehouse")
    raiz = args.saida / "landing"
    arquivos = sorted(f for f in raiz.rglob("*") if f.is_file())
    print(f"{len(arquivos)} arquivos em {raiz} -> s3://{bucket}/landing/")
    if args.so_listar:
        for f in arquivos[:40]:
            print("  landing/" + f.relative_to(raiz).as_posix())
        if len(arquivos) > 40:
            print(f"  ... e mais {len(arquivos) - 40}")
        return

    cliente = Minio(args.endpoint, access_key=env.get("MINIO_ROOT_USER", "minio_admin"),
                    secret_key=env.get("MINIO_ROOT_PASSWORD", ""), secure=False)
    if not cliente.bucket_exists(bucket):
        cliente.make_bucket(bucket)
    if args.limpar:
        n = 0
        for obj in cliente.list_objects(bucket, prefix="landing/", recursive=True):
            cliente.remove_object(bucket, obj.object_name)
            n += 1
        print(f"  removidos {n} objetos de landing/")
    for i, f in enumerate(arquivos, start=1):
        chave = "landing/" + f.relative_to(raiz).as_posix()
        tipo = mimetypes.guess_type(f.name)[0] or "application/octet-stream"
        cliente.fput_object(bucket, chave, str(f), content_type=tipo)
        if i % 200 == 0 or i == len(arquivos):
            print(f"  {i}/{len(arquivos)}")
    print("concluido")


if __name__ == "__main__":
    main()
