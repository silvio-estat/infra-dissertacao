#!/usr/bin/env python3
"""
provisionar_governanca.py — Materializa no OpenMetadata os artefatos de governanca
exigidos pelo IR 14-06 (Portaria n 011-STI, de 18 de outubro de 2004).

A norma exige que a Administracao de Dados mantenha registros formais (Art. 18):
    - Glossario de Termos ................ Art. 31
    - Dicionario de Abreviaturas ......... Art. 32 a 35
    - Termos de Representacao ............ Titulo X, Art. 138 a 143

Aqui esses tres registros deixam de ser documento avulso e passam a ser entidades
de primeira classe do catalogo, versionadas em canonico/ir_14_06/*.csv e publicadas
via API. A camada Medallion de cada tabela e gravada no campo `certification` da
entidade Table (Bronze / Silver / Gold), e nao no nome do esquema — o que libera o
nome do esquema para seguir a regra de area de negocio do Art. 27.

Roda FORA do Docker, contra o OpenMetadata em localhost:8585.
Usa apenas biblioteca padrao: nao exige o venv do projeto.

    python3 scripts/governanca/provisionar_governanca.py --tudo --dry-run
    python3 scripts/governanca/provisionar_governanca.py --tudo
"""

import argparse
import csv
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
DIR_DADOS = RAIZ / "canonico" / "ir_14_06"

URL_PADRAO = os.environ.get("OM_URL", "http://localhost:8585")
SERVICO_PADRAO = os.environ.get("OM_SERVICO", "trino_lakehouse")

# Nome do esquema -> nivel de certificacao. A camada Medallion vive aqui.
CERTIFICACAO_POR_ESQUEMA = {
    "bronze": "Certification.Bronze",
    "silver": "Certification.Silver",
    "gold": "Certification.Gold",
}

# Os tres registros do IR 14-06, na ordem em que devem ser criados.
# Art. 34: o termo precisa constar no Glossario antes de a abreviatura ser catalogada,
# por isso o glossario de termos vem primeiro.
GLOSSARIOS = [
    {
        "arquivo": "glossario.csv",
        "name": "IR1406_Glossario_de_Termos",
        "displayName": "IR 14-06 — Glossário de Termos",
        "description": (
            "Glossário de Termos previsto no Art. 31 do IR 14-06 (Normas de Atribuição de "
            "Nomes e Metadados para Administração de Dados no Exército). Contém o termo, a "
            "descrição de sua semântica e a relação de termos com mesmo significado. "
            "Pré-requisito do Dicionário de Abreviaturas (Art. 34)."
        ),
        "coluna_nome": "termo",
        "coluna_descricao": "definicao",
        "coluna_sinonimos": "sinonimos",
    },
    {
        "arquivo": "abreviaturas.csv",
        "name": "IR1406_Dicionario_de_Abreviaturas",
        "displayName": "IR 14-06 — Dicionário de Abreviaturas",
        "description": (
            "Dicionário de Abreviaturas previsto nos Art. 32 a 35 do IR 14-06. O uso de uma "
            "abreviatura fica condicionado à sua catalogação prévia neste dicionário (Art. 33). "
            "A base inicial são as abreviaturas do C 21-30. Entradas marcadas como PROPOSTA "
            "foram criadas por este trabalho e pendem de aprovação da Administração de Dados."
        ),
        "coluna_nome": "abreviatura",
        "coluna_descricao": "justificativa",
        # a grafia de banco (maiuscula, IR 14-06 Art. 67) entra como sinonimo para
        # que a busca no catalogo encontre a entrada pelas duas formas
        "coluna_sinonimos": "termo,abreviatura_bd",
    },
    {
        "arquivo": "termos_representacao.csv",
        "name": "IR1406_Termos_de_Representacao",
        "displayName": "IR 14-06 — Termos de Representação",
        "description": (
            "Termos de Representação do Título X do IR 14-06 (Art. 138). Vocabulário fechado: "
            "o nome de uma coluna admite um único termo de representação (Art. 76, III) e deve "
            "usá-lo na forma abreviada (Art. 41). Novos termos só podem ser criados por "
            "solicitação à Administração de Dados."
        ),
        "coluna_nome": "termo",
        "coluna_descricao": "definicao",
        "coluna_sinonimos": "abreviatura",
    },
]


class ErroOpenMetadata(RuntimeError):
    """Falha ao conversar com a API do OpenMetadata."""


def _token() -> str:
    token = os.environ.get("OM_INGESTION_BOT_JWT", "").strip()
    if not token:
        raise SystemExit(
            "OM_INGESTION_BOT_JWT nao definido.\n"
            "  export $(grep -v '^#' .env | xargs)   # ou exporte so essa variavel"
        )
    return token


def requisitar(metodo: str, caminho: str, corpo=None, tipo_conteudo="application/json"):
    """Chamada HTTP a API do OpenMetadata. Devolve o JSON da resposta ou None."""
    url = f"{URL_BASE}{caminho}"
    dados = json.dumps(corpo).encode("utf-8") if corpo is not None else None
    req = urllib.request.Request(url, data=dados, method=metodo)
    req.add_header("Authorization", f"Bearer {_token()}")
    req.add_header("Accept", "application/json")
    if dados is not None:
        req.add_header("Content-Type", tipo_conteudo)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            texto = resp.read().decode("utf-8")
            return json.loads(texto) if texto else None
    except urllib.error.HTTPError as erro:
        detalhe = erro.read().decode("utf-8", errors="replace")[:400]
        raise ErroOpenMetadata(f"{metodo} {caminho} -> HTTP {erro.code}: {detalhe}") from erro
    except urllib.error.URLError as erro:
        raise ErroOpenMetadata(
            f"{metodo} {caminho} -> sem resposta de {URL_BASE} ({erro.reason}). "
            "A stack esta no ar? (docker compose --profile governance up -d)"
        ) from erro


def ler_csv(nome_arquivo: str) -> list[dict]:
    """Le um CSV de canonico/ir_14_06/, ignorando as linhas de comentario iniciadas por #."""
    caminho = DIR_DADOS / nome_arquivo
    if not caminho.exists():
        raise SystemExit(f"Arquivo nao encontrado: {caminho}")
    with caminho.open(encoding="utf-8") as fh:
        linhas = [ln for ln in fh if not ln.lstrip().startswith("#")]
    return [linha for linha in csv.DictReader(linhas) if any(v.strip() for v in linha.values())]


def provisionar_glossarios(dry_run: bool) -> None:
    """Cria os tres glossarios do IR 14-06 e seus termos. Idempotente (PUT)."""
    for spec in GLOSSARIOS:
        registros = ler_csv(spec["arquivo"])
        print(f"\n=== {spec['displayName']} ({len(registros)} termos) ===")

        corpo_glossario = {
            "name": spec["name"],
            "displayName": spec["displayName"],
            "description": spec["description"],
        }
        if dry_run:
            print(f"  [dry-run] PUT /api/v1/glossaries  {spec['name']}")
        else:
            requisitar("PUT", "/api/v1/glossaries", corpo_glossario)
            print(f"  glossario publicado: {spec['name']}")

        for registro in registros:
            nome = (registro.get(spec["coluna_nome"]) or "").strip()
            if not nome:
                continue
            descricao = (registro.get(spec["coluna_descricao"]) or "").strip()
            sinonimos = []
            for coluna in spec["coluna_sinonimos"].split(","):
                bruto = (registro.get(coluna.strip()) or "").strip()
                sinonimos.extend(s.strip() for s in bruto.split(";") if s.strip())
            # a grafia de banco pode coincidir com o nome do termo
            sinonimos = [s for s in dict.fromkeys(sinonimos) if s != nome]

            corpo_termo = {
                "glossary": spec["name"],
                # O nome do termo nao aceita ponto (separador de FQN no OpenMetadata).
                "name": nome.replace(".", " "),
                "displayName": nome,
                # Art. 137 exige Nome + Definicao para todo item administrado; a API
                # tambem trata description como obrigatoria.
                "description": descricao or f"Termo {nome} do IR 14-06.",
            }
            if sinonimos:
                corpo_termo["synonyms"] = sinonimos

            if dry_run:
                sin = f"  sinonimos={sinonimos}" if sinonimos else ""
                print(f"  [dry-run] PUT /api/v1/glossaryTerms  {nome}{sin}")
                continue
            try:
                requisitar("PUT", "/api/v1/glossaryTerms", corpo_termo)
                print(f"  termo: {nome}")
            except ErroOpenMetadata as erro:
                print(f"  FALHA no termo {nome}: {erro}", file=sys.stderr)


GLOSSARIO_MD33 = {
    "name": "MD33_M_02_Abreviaturas_e_Siglas",
    "displayName": "MD33-M-02 — Abreviaturas e Siglas",
    "description": (
        "Base herdada do Dicionário de Abreviaturas do IR 14-06. O Art. 32 determina que "
        "as primeiras abreviaturas a catalogar sejam as do manual de abreviaturas das Forças "
        "Armadas; aqui estão as do MD33-M-02 (3ª Edição/2008), Capítulo IV, extraídas por "
        "scripts/governanca/extrair_abreviaturas_md33.py. Abreviaturas próprias deste "
        "trabalho ficam no glossário IR 14-06 — Dicionário de Abreviaturas."
    ),
}

# Caracteres que nao podem compor o nome de um termo (o ponto separa o FQN no OpenMetadata).
_INVALIDOS = str.maketrans({c: "-" for c in "./%&()"})


# Abreviaturas simbolicas nao sobrevivem a sanitizacao; recebem nome por extenso.
_SIMBOLICAS = {"%": "PORCENTO"}


def _nome_seguro(abreviatura: str) -> str:
    if abreviatura in _SIMBOLICAS:
        return _SIMBOLICAS[abreviatura]
    return re.sub(r"-+", "-", abreviatura.translate(_INVALIDOS)).strip("- ")


def provisionar_abreviaturas_md33(dry_run: bool) -> None:
    """Carga em lote do MD33-M-02. Abreviaturas repetidas viram um termo so.

    MD33-M-02, 3.1.17: a duplicidade de simbologia e mantida por ser consagrada, e o
    contexto direciona o significado. Traduzido para o catalogo: um termo por
    abreviatura, com todos os significados listados.
    """
    registros = ler_csv("abreviaturas_md33.csv")
    if not registros:
        print("abreviaturas_md33.csv vazio — rode extrair_abreviaturas_md33.py antes.")
        return

    agrupado: dict[str, list[dict]] = {}
    for reg in registros:
        chave = _nome_seguro(reg["abreviatura"])
        if chave:
            agrupado.setdefault(chave, []).append(reg)

    colisoes = sum(1 for v in agrupado.values() if len(v) > 1)
    print(f"\n=== {GLOSSARIO_MD33['displayName']} ===")
    print(f"  {len(registros)} entradas -> {len(agrupado)} termos ({colisoes} com duplicidade de simbologia)")

    if dry_run:
        print(f"  [dry-run] PUT /api/v1/glossaries  {GLOSSARIO_MD33['name']}")
        print(f"  [dry-run] PUT /api/v1/glossaryTerms  x{len(agrupado)}")
        for chave in list(agrupado)[:3]:
            print(f"      exemplo: {chave} -> {[r['termo'] for r in agrupado[chave]]}")
        return

    requisitar("PUT", "/api/v1/glossaries", GLOSSARIO_MD33)
    print(f"  glossario publicado: {GLOSSARIO_MD33['name']}")

    gravados = falhas = 0
    for i, (chave, regs) in enumerate(sorted(agrupado.items()), 1):
        significados = [r["termo_completo"] or r["termo"] for r in regs]
        if len(significados) == 1:
            descricao = significados[0]
        else:
            # 3.1.17: o texto direciona o significado identificador da sigla
            itens = "; ".join(f"({n}) {s}" for n, s in enumerate(significados, 1))
            descricao = f"Duplicidade de simbologia (MD33-M-02, 3.1.17) — {itens}"
        if any(r["somente_conjunto"] for r in regs):
            descricao += " [Somente em conjunto com outra abreviatura (MD33-M-02, 3.1.10).]"

        sinonimos = []
        for r in regs:
            sinonimos.append(r["termo"])
            sinonimos.extend(x for x in r["sinonimos"].split(";") if x)

        corpo = {
            "glossary": GLOSSARIO_MD33["name"],
            "name": chave,
            "displayName": regs[0]["abreviatura"],
            "description": descricao,
            # o OpenMetadata rejeita sinonimo repetido
            "synonyms": list(dict.fromkeys(s.strip() for s in sinonimos if s.strip()))[:50],
        }
        try:
            requisitar("PUT", "/api/v1/glossaryTerms", corpo)
            gravados += 1
        except ErroOpenMetadata as erro:
            falhas += 1
            if falhas <= 5:
                print(f"  FALHA em {chave}: {erro}", file=sys.stderr)
        if i % 250 == 0:
            print(f"  ... {i}/{len(agrupado)}")

    print(f"  {gravados} termos publicados, {falhas} falha(s).")


def listar_tabelas(servico: str) -> list[dict]:
    """Devolve todas as tabelas do servico, paginando."""
    tabelas, depois = [], None
    while True:
        params = {"service": servico, "limit": "100", "fields": "certification"}
        if depois:
            params["after"] = depois
        resposta = requisitar("GET", f"/api/v1/tables?{urllib.parse.urlencode(params)}")
        tabelas.extend(resposta.get("data", []))
        depois = (resposta.get("paging") or {}).get("after")
        if not depois:
            return tabelas


def aplicar_certificacao(servico: str, dry_run: bool) -> None:
    """Grava a camada Medallion no campo `certification` de cada tabela."""
    print(f"\n=== Certificacao por camada (servico {servico}) ===")

    try:
        requisitar("GET", "/api/v1/classifications/name/Certification")
    except ErroOpenMetadata as erro:
        print(
            "  A classificacao 'Certification' nao respondeu — sem ela nao ha como certificar.\n"
            f"  {erro}",
            file=sys.stderr,
        )
        return

    tabelas = listar_tabelas(servico)
    if not tabelas:
        print(f"  Nenhuma tabela encontrada no servico '{servico}'.")
        print("  Rode antes a ingestao de metadados (dag_trino_governance).")
        return

    aplicadas = ignoradas = inalteradas = 0
    for tabela in tabelas:
        fqn = tabela.get("fullyQualifiedName", "")
        # FQN = servico.banco.esquema.tabela
        partes = fqn.split(".")
        esquema = partes[2].lower() if len(partes) >= 4 else ""
        alvo = CERTIFICACAO_POR_ESQUEMA.get(esquema)
        if not alvo:
            ignoradas += 1
            continue

        atual = ((tabela.get("certification") or {}).get("tagLabel") or {}).get("tagFQN")
        if atual == alvo:
            inalteradas += 1
            continue

        patch = [
            {
                "op": "add",
                "path": "/certification",
                "value": {"tagLabel": {"tagFQN": alvo, "source": "Classification"}},
            }
        ]
        if dry_run:
            print(f"  [dry-run] PATCH {fqn} -> {alvo}")
            aplicadas += 1
            continue
        try:
            requisitar(
                "PATCH",
                f"/api/v1/tables/{tabela['id']}",
                patch,
                tipo_conteudo="application/json-patch+json",
            )
            print(f"  {fqn} -> {alvo}")
            aplicadas += 1
        except ErroOpenMetadata as erro:
            print(f"  FALHA em {fqn}: {erro}", file=sys.stderr)

    print(
        f"\n  {aplicadas} certificada(s), {inalteradas} ja corretas, "
        f"{ignoradas} fora das camadas mapeadas."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--glossarios", action="store_true", help="publica os tres registros do IR 14-06")
    parser.add_argument("--certificacao", action="store_true", help="grava a camada Medallion nas tabelas")
    parser.add_argument("--md33", action="store_true",
                        help="carga em lote do MD33-M-02 (4127 termos, alguns minutos)")
    parser.add_argument("--tudo", action="store_true",
                        help="equivale a --glossarios --md33 --certificacao")
    parser.add_argument("--dry-run", action="store_true", help="mostra o que faria, sem escrever")
    parser.add_argument("--url", default=URL_PADRAO, help=f"URL do OpenMetadata (padrao: {URL_PADRAO})")
    parser.add_argument("--servico", default=SERVICO_PADRAO, help=f"servico no OM (padrao: {SERVICO_PADRAO})")
    args = parser.parse_args()

    if not (args.glossarios or args.certificacao or args.md33 or args.tudo):
        parser.error("escolha --glossarios, --md33, --certificacao ou --tudo")

    global URL_BASE
    URL_BASE = args.url.rstrip("/")

    if args.dry_run:
        print(">>> MODO DRY-RUN: nada sera escrito no OpenMetadata <<<")

    try:
        if args.glossarios or args.tudo:
            provisionar_glossarios(args.dry_run)
        if args.md33 or args.tudo:
            provisionar_abreviaturas_md33(args.dry_run)
        if args.certificacao or args.tudo:
            aplicar_certificacao(args.servico, args.dry_run)
    except ErroOpenMetadata as erro:
        raise SystemExit(f"\nErro: {erro}")


if __name__ == "__main__":
    main()
