"""
Linhagem — avisa o OpenMetadata de que uma tarefa leu umas tabelas e escreveu outras.

A declaracao de "quem le o que e escreve o que" fica DENTRO de cada DAG, ao lado
da tarefa. Este modulo so faz o encanamento: monta a mensagem no formato
OpenLineage e a envia ao OpenMetadata, que desenha a seta no grafo.

Uso, na DAG:

    extrair = PythonOperator(
        task_id="extrair_planilhas",
        python_callable=extrair_planilhas,
        on_success_callback=linhagem(le=["bronze.arquivo"], escreve=["bronze.extracao"]),
    )

Opcionais: `sql` (a consulta que fez a transformacao, mostrada na seta) e
`colunas` (linhagem coluna a coluna: {"coluna_de_saida": [("bronze.arquivo", "coluna_de_entrada")]}).

Os nomes vao como lakehouse.<esquema>.<tabela> — o mesmo catalogo que o Spark usa —
com namespace `dlh`; e assim que o OpenMetadata 1.12.5 encontra a tabela. (O listener
OpenLineage do Spark emite namespace hive://, que ele nao resolve; por isso o aviso
e feito daqui.)
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)

OM_URL = os.environ.get("OPENMETADATA_URL", "http://openmetadata:8585")
OM_ENDPOINT = f"{OM_URL}/api/v1/openlineage/lineage"
OM_JWT = os.environ.get("OM_INGESTION_BOT_JWT", "")
NAMESPACE = "dlh"
CATALOGO = "lakehouse"
PRODUTOR = "https://github.com/infra-dissertacao/lineage_emitter"


def linhagem(le: list[str], escreve: list[str], sql: str | None = None,
             colunas: dict[str, list[tuple[str, str]]] | None = None):
    """Devolve o callback que a tarefa chama ao terminar com sucesso."""
    def _ao_terminar(context):
        ti = context["task_instance"]
        emitir(le, escreve, f"{ti.dag_id}.{ti.task_id}", sql, colunas)
    return _ao_terminar


def emitir(le: list[str], escreve: list[str], tarefa: str,
           sql: str | None = None, colunas: dict | None = None) -> bool:
    """Envia um evento OpenLineage COMPLETE. Devolve True se o OM criou aresta."""
    if not OM_JWT:
        log.warning("OM_INGESTION_BOT_JWT nao definido — linhagem nao enviada")
        return False

    facetas_tarefa = {"sql": _faceta_sql(sql)} if sql else {}
    facetas_saida = {"columnLineage": _faceta_colunas(colunas)} if colunas else {}
    evento = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run": {"runId": str(uuid.uuid4()), "facets": {}},
        "job": {"namespace": NAMESPACE, "name": tarefa, "facets": facetas_tarefa},
        "inputs": [{"namespace": NAMESPACE, "name": _nome(t), "facets": {}} for t in le],
        "outputs": [{"namespace": NAMESPACE, "name": _nome(t), "facets": facetas_saida} for t in escreve],
        "producer": PRODUTOR,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
    }
    return _enviar(evento)


def _nome(tabela: str) -> str:
    """'bronze.arquivo' -> 'lakehouse.bronze.arquivo'."""
    return tabela if tabela.startswith(CATALOGO + ".") else f"{CATALOGO}.{tabela}"


def _faceta_sql(sql: str) -> dict:
    return {"_producer": PRODUTOR,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json#/$defs/SQLJobFacet",
            "query": sql}


def _faceta_colunas(colunas: dict) -> dict:
    campos = {
        saida: {
            "inputFields": [{"namespace": NAMESPACE, "name": _nome(t), "field": c} for t, c in origens],
            "transformationType": "INDIRECT",
            "transformationDescription": "",
        }
        for saida, origens in colunas.items()
    }
    return {"_producer": PRODUTOR,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-2/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
            "fields": campos}


def _enviar(evento: dict) -> bool:
    req = Request(OM_ENDPOINT, data=json.dumps(evento).encode(), method="POST",
                  headers={"Content-Type": "application/json", "Authorization": f"Bearer {OM_JWT}"})
    try:
        with urlopen(req, timeout=10) as resp:
            arestas = json.loads(resp.read()).get("lineageEdgesCreated", 0)
            log.info("linhagem %s -> OpenMetadata: %d aresta(s)", evento["job"]["name"], arestas)
            return arestas > 0
    except Exception:
        log.exception("falha ao enviar linhagem ao OpenMetadata")
        return False
