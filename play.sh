#!/bin/bash
# =============================================================================
# play.sh — prepara a maquina para rodar o estudo, do zero.
#
#     bash play.sh                 # prepara tudo
#     bash play.sh --escala 0.1    # corpus reduzido, para conferir o caminho
#     bash play.sh --conferir      # so diz o que falta, sem mudar nada
#
# Por que este arquivo existe: os dados sinteticos (254 MB) e os modelos de IA
# (alguns GB) NAO ficam no repositorio. Guardar binario grande no Git e ruim
# duas vezes — ele nunca sai do historico e nao comprime entre versoes. Em vez
# disso, o repositorio guarda quem SABE produzi-los, e este script os chama na
# ordem certa. E o "baixador" de que a pessoa que clonou precisa.
#
# O script e idempotente: rodar de novo pula o que ja esta pronto. Pode ser
# interrompido e retomado.
#
# FASE 1 (passos 1 a 5): a maquina. FASE 2 (6 a 12): a stack, o catalogo e as
# tabelas. O pipeline em si (DAGs 1 a 5) so roda com --rodar-dags, porque a DAG 2
# leva de minutos (com GPU) a algumas horas (so em CPU) e nao convem escondida
# dentro de um script.
# =============================================================================

set -euo pipefail
cd "$(dirname "$0")"

ESCALA=""
SO_CONFERIR=0
RODAR_DAGS=0
while [ $# -gt 0 ]; do
    case "$1" in
        --escala)     ESCALA="$2"; shift 2 ;;
        --conferir)   SO_CONFERIR=1; shift ;;
        --rodar-dags) RODAR_DAGS=1; shift ;;
        *) echo "opcao desconhecida: $1"; exit 1 ;;
    esac
done

PASSO=0
passo()  { PASSO=$((PASSO + 1)); echo; echo "[$PASSO] $1"; }
ok()     { echo "    ok: $1"; }
pular()  { echo "    ja pronto: $1"; }
faz()    { echo "    fazendo: $1"; }
erro()   { echo; echo "ERRO: $1"; echo; exit 1; }

echo "============================================"
echo " infra-dissertacao — preparacao do ambiente"
echo "============================================"
[ "$SO_CONFERIR" = 1 ] && echo "(modo --conferir: nada sera alterado)"

# -----------------------------------------------------------------------------
passo "Pre-requisitos"

command -v docker >/dev/null || erro "Docker nao encontrado. Instale o Docker com Compose v2."
docker compose version >/dev/null 2>&1 \
    || erro "Docker Compose v2 nao encontrado (o comando e 'docker compose', com espaco)."
ok "docker $(docker --version | awk '{print $3}' | tr -d ,)"

command -v python3 >/dev/null || erro "python3 nao encontrado. E preciso Python 3.10 ou mais novo."
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)' \
    || erro "Python $(python3 -V | awk '{print $2}') e velho demais. Minimo: 3.10."
ok "python $(python3 -V | awk '{print $2}')"

# O Ollama roda NA MAQUINA, fora do Docker: no Docker Desktop o conteiner nao
# enxerga a placa de video. As DAGs falam com ele por host.docker.internal:11434.
command -v ollama >/dev/null \
    || erro "Ollama nao encontrado. Instale de https://ollama.com e rode este script de novo."
ok "ollama $(ollama --version 2>/dev/null | awk '{print $NF}')"

# -----------------------------------------------------------------------------
passo "Arquivos de configuracao"

if [ -f .env ]; then
    pular ".env"
else
    faz ".env (copia de .env.example)"
    [ "$SO_CONFERIR" = 1 ] || cp .env.example .env
fi

# Tem de existir ANTES do primeiro 'docker compose up'. Se faltar, o Docker cria
# uma PASTA com esse nome no lugar do arquivo e a stack passa a nao subir — a
# armadilha de montagem descrita no README.
if [ -f infra/openlineage/openlineage.yml ]; then
    pular "infra/openlineage/openlineage.yml"
else
    faz "infra/openlineage/openlineage.yml (copia do .example)"
    [ "$SO_CONFERIR" = 1 ] || cp infra/openlineage/openlineage.yml.example infra/openlineage/openlineage.yml
    echo "    aviso: o apiKey dentro dele ainda e o de exemplo. Trocar pelo"
    echo "           OM_INGESTION_BOT_JWT depois que o OpenMetadata subir."
fi

# -----------------------------------------------------------------------------
passo "Modelo de linguagem (Ollama)"

MODELO_LLM=$(grep -oP 'LLM_MODELO\s*=\s*"\K[^"]+' airflow/dags/2_bronze_extracao.py | head -1)
[ -n "$MODELO_LLM" ] || erro "nao consegui ler LLM_MODELO de airflow/dags/2_bronze_extracao.py"

if ollama list 2>/dev/null | awk '{print $1}' | grep -qx "$MODELO_LLM"; then
    pular "$MODELO_LLM"
else
    faz "ollama pull $MODELO_LLM (~3,4 GB)"
    [ "$SO_CONFERIR" = 1 ] || ollama pull "$MODELO_LLM"
fi

# -----------------------------------------------------------------------------
passo "Ambiente Python dos geradores"

# Venv proprio, separado do venv/ do projeto: os geradores precisam de Pillow,
# piper-tts e openpyxl, que nada mais usa.
PY_GER="./venv-geradores/bin/python"
if [ -x "$PY_GER" ]; then
    pular "venv-geradores/"
else
    faz "venv-geradores/ + requirements-geradores.txt"
    if [ "$SO_CONFERIR" = 0 ]; then
        python3 -m venv venv-geradores
        ./venv-geradores/bin/pip install --quiet --upgrade pip
        ./venv-geradores/bin/pip install --quiet -r requirements-geradores.txt
    fi
fi

# -----------------------------------------------------------------------------
passo "Corpus sintetico"

# A semente e fixa, entao o CENARIO se repete: os mesmos fatos, as mesmas
# unidades, os mesmos defeitos plantados. Mas xlsx, pdf e wav NAO saem byte a
# byte iguais ao regerar (carimbo de hora, antialiasing, sintese de voz).
if [ -d dados_sinteticos/landing ] && [ -f dados_sinteticos/verdade/fatos.csv ]; then
    pular "dados_sinteticos/ ($(du -sh dados_sinteticos 2>/dev/null | cut -f1))"
    echo "    para refazer do zero: rm -rf dados_sinteticos && bash play.sh"
else
    faz "gerar_tudo.py (~80 s; baixa as vozes do Piper na primeira vez, ~120 MB)"
    [ "$SO_CONFERIR" = 1 ] || $PY_GER scripts/geradores/gerar_tudo.py ${ESCALA:+--escala "$ESCALA"}
fi

if [ "$SO_CONFERIR" = 1 ]; then
    echo
    echo "============================================"
    echo " conferencia da fase 1 concluida"
    echo "============================================"
    echo "(a fase 2 nao e conferida: ela depende da stack no ar)"
    exit 0
fi

# =============================================================================
# FASE 2 — a stack, o catalogo e as tabelas
# =============================================================================

# Espera uma URL responder. O OpenMetadata leva alguns minutos para subir, e
# nada adiante funciona antes dele.
esperar_url() {
    local url="$1" nome="$2" limite="${3:-300}" t=0
    while ! curl -sf -o /dev/null --max-time 5 "$url"; do
        t=$((t + 5))
        [ "$t" -ge "$limite" ] && erro "$nome nao respondeu em ${limite}s ($url)"
        [ $((t % 30)) -eq 0 ] && echo "    esperando $nome... ${t}s"
        sleep 5
    done
    ok "$nome respondendo"
}

# Grava CHAVE=valor no .env, trocando a linha se ela ja existir.
gravar_env() {
    local chave="$1" valor="$2"
    if grep -q "^${chave}=" .env; then
        python3 - "$chave" "$valor" <<'PY'
import sys, pathlib
chave, valor = sys.argv[1], sys.argv[2]
p = pathlib.Path(".env")
linhas = [f"{chave}={valor}" if l.startswith(f"{chave}=") else l
          for l in p.read_text(encoding="utf-8").splitlines()]
p.write_text("\n".join(linhas) + "\n", encoding="utf-8")
PY
    else
        printf '%s=%s\n' "$chave" "$valor" >> .env
    fi
}

# -----------------------------------------------------------------------------
passo "Subir a stack"

# --build porque as imagens do Airflow e do Spark sao proprias (infra/*/Dockerfile).
# Na primeira vez isso demora bastante; depois o cache resolve em segundos.
faz "docker compose up -d --build (a primeira vez constroi as imagens)"
docker compose up -d --build

esperar_url "http://localhost:9000/minio/health/live" "MinIO" 120
esperar_url "http://localhost:8090/v1/info"           "Trino" 180
esperar_url "http://localhost:8080/health"            "Airflow" 300
esperar_url "http://localhost:8585/api/v1/system/version" "OpenMetadata" 600

# -----------------------------------------------------------------------------
passo "Token do robo de ingestao do OpenMetadata"

# O token do ingestion-bot e proprio de cada instalacao e muda a cada
# 'docker compose down -v'. Ele vai para DOIS lugares: OM_INGESTION_BOT_JWT no
# .env (que as DAGs leem) e apiKey no openlineage.yml (que o Spark le).
OM_ADMIN_SENHA=$(printf admin | base64)
TOKEN_ADMIN=$(curl -s -X POST http://localhost:8585/api/v1/users/login \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"admin@open-metadata.org\",\"password\":\"$OM_ADMIN_SENHA\"}" \
    | python3 -c "import sys,json;print(json.load(sys.stdin).get('accessToken',''))")
[ -n "$TOKEN_ADMIN" ] || erro "nao consegui autenticar no OpenMetadata como admin@open-metadata.org"

BOT_ID=$(curl -s -H "Authorization: Bearer $TOKEN_ADMIN" \
    http://localhost:8585/api/v1/bots/name/ingestion-bot \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['botUser']['id'])")
JWT=$(curl -s -H "Authorization: Bearer $TOKEN_ADMIN" \
    "http://localhost:8585/api/v1/users/auth-mechanism/$BOT_ID" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['config']['JWTToken'])")
[ -n "$JWT" ] || erro "nao consegui ler o JWT do ingestion-bot"

JWT_ATUAL=$(grep -oP '^OM_INGESTION_BOT_JWT=\K.*' .env 2>/dev/null || true)
if [ "$JWT" = "$JWT_ATUAL" ]; then
    pular "token ja gravado e atual"
else
    faz "gravando o token no .env e no openlineage.yml"
    gravar_env OM_INGESTION_BOT_JWT "$JWT"
    python3 - "$JWT" <<'PY'
import sys, pathlib, re
# Ancorado na linha (^...$ com re.M) e count=1: sem isso, um 'apiKey:' sem valor
# faria o \s* engolir a quebra de linha e levar junto a linha seguinte.
p = pathlib.Path("infra/openlineage/openlineage.yml")
novo, n = re.subn(r"^(\s*apiKey:).*$", r"\1 " + sys.argv[1],
                  p.read_text(encoding="utf-8"), count=1, flags=re.M)
if n != 1:
    sys.exit("nao achei a linha 'apiKey:' em infra/openlineage/openlineage.yml")
p.write_text(novo, encoding="utf-8")
PY
    # Os conteineres leem o token no arranque: sem recriar, seguem com o antigo.
    # 'up -d' e nao 'restart' — restart nao reaplica variavel de ambiente.
    faz "recriando os servicos que usam o token"
    docker compose up -d --force-recreate airflow-scheduler airflow-webserver spark-master spark-worker
    esperar_url "http://localhost:8080/health" "Airflow" 300
fi

# -----------------------------------------------------------------------------
passo "Servico trino_lakehouse no OpenMetadata"

# Sem este servico a linhagem nao tem onde pousar: lineageEdgesCreated fica 0.
# Pela tela seria Settings -> Services -> Databases -> Add New Service -> Trino.
if curl -sf -o /dev/null -H "Authorization: Bearer $JWT" \
     "http://localhost:8585/api/v1/services/databaseServices/name/trino_lakehouse"; then
    pular "servico trino_lakehouse"
else
    faz "criando o servico trino_lakehouse"
    curl -sf -X POST "http://localhost:8585/api/v1/services/databaseServices" \
        -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
        -d '{
              "name": "trino_lakehouse",
              "serviceType": "Trino",
              "description": "Trino sobre o Lakehouse Iceberg (catalogo iceberg)",
              "connection": {"config": {
                  "type": "Trino", "scheme": "trino", "username": "trino",
                  "hostPort": "trino:8090", "catalog": "iceberg",
                  "connectionArguments": {"http_scheme": "http"}}}
            }' >/dev/null || erro "falha ao criar o servico trino_lakehouse"
    ok "servico criado"
fi

# -----------------------------------------------------------------------------
passo "Tabelas e tabelas de referencia"

set -a; . ./.env; set +a

# --ddl cria as 18 tabelas a partir de canonico/modelo_canonico.yaml;
# --seeds carrega as quatro REF_* de canonico/seeds/*.csv. Os dois sao
# idempotentes: CREATE TABLE IF NOT EXISTS e upsert pela chave.
for etapa in --ddl --seeds; do
    faz "canonico_para_silver.py $etapa"
    docker exec dlh_spark_master /opt/spark/bin/spark-submit --master "local[2]" \
        --conf spark.hadoop.fs.s3a.access.key="$MINIO_ROOT_USER" \
        --conf spark.hadoop.fs.s3a.secret.key="$MINIO_ROOT_PASSWORD" \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
        /opt/spark-jobs/canonico_para_silver.py "$etapa" >/dev/null 2>&1 \
        || erro "canonico_para_silver.py $etapa falhou. Rode-o a mao para ver o motivo."
    ok "$etapa"
done

# -----------------------------------------------------------------------------
passo "Enviar a landing para o MinIO"

# A ingestao deduplica pelo hash, entao reenviar o mesmo arquivo nao gera linha
# nova na Bronze.
faz "enviar_landing.py"
$PY_GER scripts/geradores/enviar_landing.py >/dev/null || erro "envio para o MinIO falhou"
ok "landing enviada"

# -----------------------------------------------------------------------------
passo "Catalogar as tabelas no OpenMetadata"

# Antes disto o OM devolve HTTP 404 ao criar teste de qualidade, e a linhagem
# nao cria aresta nenhuma.
disparar_dag() {
    local dag="$1" limite="${2:-1800}" t=0 estado=""
    docker exec dlh_airflow_scheduler airflow dags unpause "$dag" >/dev/null 2>&1 || true
    docker exec dlh_airflow_scheduler airflow dags trigger "$dag" >/dev/null 2>&1 \
        || erro "nao consegui disparar a DAG $dag"
    while true; do
        estado=$(docker exec dlh_airflow_scheduler airflow dags list-runs -d "$dag" \
                    --no-backfill -o plain 2>/dev/null | awk 'NR==2{print $3}')
        case "$estado" in
            success) ok "$dag: success (${t}s)"; return 0 ;;
            failed)  erro "a DAG $dag falhou. Veja o log no Airflow (http://localhost:8080)." ;;
        esac
        t=$((t + 10))
        [ "$t" -ge "$limite" ] && erro "a DAG $dag passou de ${limite}s sem terminar"
        [ $((t % 60)) -eq 0 ] && echo "    $dag rodando... ${t}s"
        sleep 10
    done
}

faz "DAG trino_lakehouse_metadata"
disparar_dag trino_lakehouse_metadata 900

# -----------------------------------------------------------------------------
passo "Registros do IR 14-06 e testes de qualidade"

# Os dois rodam na maquina, so com biblioteca padrao, e sao idempotentes por nome.
faz "provisionar_governanca.py --tudo"
python3 scripts/governanca/provisionar_governanca.py --tudo >/dev/null \
    || echo "    aviso: provisionar_governanca.py falhou; siga sem ele e rode a mao depois"
faz "testes_qualidade.py"
python3 scripts/testes_qualidade.py >/dev/null \
    || echo "    aviso: testes_qualidade.py falhou; siga sem ele e rode a mao depois"
ok "governanca provisionada"

# -----------------------------------------------------------------------------
if [ "$RODAR_DAGS" = 1 ]; then
    passo "Pipeline (DAGs 1 a 5)"
    # Em serie e nesta ordem: cada camada e funcao da anterior.
    for dag in 1_ingestao 2_bronze_extracao 3_silver_evento 4_gold_visoes 5_governanca; do
        if [ "$dag" = "2_bronze_extracao" ]; then
            echo "    atencao: a DAG 2 transcreve 150 audios e faz ~480 chamadas ao modelo"
            echo "             de linguagem. Com GPU, minutos; so em CPU, algumas horas."
        fi
        faz "DAG $dag"
        disparar_dag "$dag" 28800
    done
fi

# -----------------------------------------------------------------------------
echo
echo "============================================"
echo " ambiente pronto"
echo "============================================"
echo
echo "  MinIO ......... http://localhost:9001"
echo "  Airflow ....... http://localhost:8080   (admin / admin)"
echo "  OpenMetadata .. http://localhost:8585   (admin@open-metadata.org / admin)"
echo "  Spark ......... http://localhost:8081"
echo
if [ "$RODAR_DAGS" = 1 ]; then
    echo "O pipeline rodou de ponta a ponta. Para conferir:"
    echo
    echo "  docker exec dlh_trino trino --server localhost:8090 --execute \\"
    echo "    \"SELECT sistema_origem_cod, modalidade_origem_cod, count(*)"
    echo "     FROM iceberg.silver.evento GROUP BY 1, 2 ORDER BY 1\""
else
    echo "Falta rodar o pipeline. Ou 'bash play.sh --rodar-dags', ou uma de cada"
    echo "vez, esperando a anterior terminar:"
    echo
    for dag in 1_ingestao 2_bronze_extracao 3_silver_evento 4_gold_visoes 5_governanca; do
        echo "  docker exec dlh_airflow_scheduler airflow dags trigger $dag"
    done
fi
