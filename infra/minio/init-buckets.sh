#!/bin/sh
# Inicializa os buckets MinIO necessários para o projeto

set -e

MC=/usr/bin/mc

echo "Aguardando MinIO ficar disponível..."
until $MC alias set myminio "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
    sleep 2
done

echo "MinIO disponível. Criando buckets..."

# landing: recebe os JSONs brutos do gerador de dados (entrada do pipeline)
$MC mb --ignore-existing myminio/landing
$MC anonymous set none myminio/landing

# lakehouse: warehouse Iceberg (Bronze, Silver, Gold armazenados aqui)
$MC mb --ignore-existing myminio/lakehouse
$MC anonymous set none myminio/lakehouse

# Prefixos dentro do lakehouse (organizacionais, não são buckets separados)
# myminio/lakehouse/bronze/
# myminio/lakehouse/silver/
# myminio/lakehouse/gold/
# myminio/lakehouse/warehouse/ (metadados Iceberg)

echo "Buckets criados com sucesso:"
$MC ls myminio
