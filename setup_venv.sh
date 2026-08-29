#!/bin/bash
# ============================================================
# Cria e configura o ambiente virtual Python do projeto
# Uso: bash setup_venv.sh
# ============================================================

set -e

VENV_DIR="venv"
PYTHON_MIN="3.10"

echo "=== infra-dissertacao — Setup do ambiente virtual ==="

# Verifica versão do Python
PYTHON=$(which python3)
PYTHON_VERSION=$($PYTHON --version 2>&1 | awk '{print $2}')
echo "Python encontrado: $PYTHON_VERSION"

if python3 -c "import sys; exit(0 if sys.version_info >= (3,10) else 1)"; then
    echo "Versão OK (>= $PYTHON_MIN)"
else
    echo "ERRO: Python >= $PYTHON_MIN é necessário."
    exit 1
fi

# Cria o venv (se não existir)
if [ ! -d "$VENV_DIR" ]; then
    echo "Criando venv em ./$VENV_DIR..."
    python3 -m venv "$VENV_DIR"
else
    echo "venv já existe em ./$VENV_DIR — reutilizando."
fi

# Ativa o venv
source "$VENV_DIR/bin/activate"

echo "Atualizando pip..."
pip install --upgrade pip --quiet

echo "Instalando dependências de requirements.txt..."
pip install -r requirements.txt

echo ""
echo "=== Setup concluído! ==="
echo "Para ativar o venv: source venv/bin/activate"
echo "Para subir a stack:  docker compose up -d"
echo "Para verificar:      docker compose ps"
