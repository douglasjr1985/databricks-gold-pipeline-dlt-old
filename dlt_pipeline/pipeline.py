# dlt_pipeline/pipeline.py

import requests
import json
import os


class DeltaLiveTablesPipeline:
    def __init__(self, token, host):
        """
        Inicializa a classe com token de autenticação e URL da instância Databricks.

        Args:
            token (str): Token de acesso para autenticação.
            host (str): URL do host Databricks.
        """
        self.token = token
        self.host = host
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def create_pipeline_payload(self, name, target, sql_paths, num_workers=2, trigger_interval="1 hour"):
        """
        Cria o payload JSON para a criação de um pipeline Delta Live Tables.

        Args:
            name (str): Nome do pipeline.
            target (str): Nome do alvo (target) do Delta Live Tables.
            sql_paths (list): Lista de caminhos dos arquivos SQL.
            num_workers (int): Número de trabalhadores no cluster.
            trigger_interval (str): Intervalo de gatilho para execução do pipeline.

        Returns:
            dict: Payload JSON.
        """
        libraries = [{"file": {"path": path}} for path in sql_paths]
        return {
            "name": name,
            "target": target,
            "libraries": libraries,
            "clusters": [
                {
                    "label": "default",
                    "num_workers": num_workers
                }
            ],
            "configuration": {
                "pipelines.trigger.interval": trigger_interval
            }
        }

    def create_pipeline(self, payload):
        """
        Cria um pipeline Delta Live Tables no Databricks.

        Args:
            payload (dict): Payload JSON para a criação do pipeline.

        Returns:
            dict: Resposta da API.
        """
        url = f"https://{self.host}/api/2.0/pipelines"
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro ao criar o pipeline: {response.text}")

    def start_pipeline(self, pipeline_id):
        """
        Inicia a execução de um pipeline Delta Live Tables no Databricks.

        Args:
            pipeline_id (str): ID do pipeline a ser iniciado.

        Returns:
            dict: Resposta da API.
        """
        url = f"https://{self.host}/api/2.0/pipelines/{pipeline_id}/updates"
        response = requests.post(url, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro ao iniciar o pipeline: {response.text}")

    def get_pipeline_status(self, pipeline_id):
        """
        Obtém o status de um pipeline Delta Live Tables no Databricks.

        Args:
            pipeline_id (str): ID do pipeline.

        Returns:
            dict: Resposta da API.
        """
        url = f"https://{self.host}/api/2.0/pipelines/{pipeline_id}"
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Erro ao obter o status do pipeline: {response.text}")
