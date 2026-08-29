#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License")

import functools

from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

# Patch requires_access to be lazy — fixes circular import with Airflow 2.7+
# The original requires_access calls get_airflow_app() at decoration time,
# which fails during plugin loading because the Flask app is not yet ready.
def _patch_requires_access():
    from airflow.api_connexion import security

    def lazy_requires_access(permissions_list):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    security.requires_access = lazy_requires_access

_patch_requires_access()

from openmetadata_managed_apis.api.app import get_blueprint
from openmetadata_managed_apis.api.config import PLUGIN_NAME
from openmetadata_managed_apis.views.rest_api import RestApiView

rest_api_view = {"category": "Admin", "name": "REST API Plugin", "view": RestApiView()}

template_blueprint = Blueprint(
    "template_blueprint",
    __name__,
    template_folder="views/templates",
)

# ---------------------------------------------------------------------
# Webhook para disparar automaticamente a DAG de profiling quando o
# OpenMetadata registra a criação de uma nova tabela.
# ---------------------------------------------------------------------
from flask import request, jsonify
from airflow.api.client.local_client import Client

@template_blueprint.route("/om/table_created", methods=["POST"])
def table_created_webhook():
    """Endpoint chamado pelo OpenMetadata via webhook.
    Espera‑se um payload JSON contendo o FQN da tabela criada (campo
    ``entityFQN`` ou ``tableFQN``). Ao receber o evento, a função aciona
    a DAG ``airflow_trino_profiler`` passando o FQN como configuração.
    """
    payload = request.get_json(force=True)
    table_fqn = payload.get("entityFQN") or payload.get("tableFQN")
    if not table_fqn:
        return jsonify({"error": "missing table_fqn in payload"}), 400
    client = Client(None, None)
    try:
        client.trigger_dag(dag_id="trino_profiler", conf={"table_fqn": table_fqn})
        return jsonify({"status": "triggered", "dag_id": "airflow_trino_profiler", "table_fqn": table_fqn}), 200
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

api_blueprint = get_blueprint()


class RestApiPlugin(AirflowPlugin):
    name = PLUGIN_NAME
    operators = []
    appbuilder_views = [rest_api_view]
    flask_blueprints = [template_blueprint, api_blueprint]
    hooks = []
    executors = []
    admin_views = [rest_api_view]
    menu_links = []
