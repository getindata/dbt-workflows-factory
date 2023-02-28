import json
import logging

from src.DbtWorkflowsConverter.dag_factory.dbt_graph_factory import DbtGraphFactory
from src.DbtWorkflowsConverter.flow_builder import FlowBuilder


class DbtWorkflowsConverter:
    dbt_manifest_path: str
    env: str
    workflow_config_file_name: str
    _builder: FlowBuilder

    def __init__(
        self,
        dbt_manifest_path: str,
        env: str,
        workflow_config_file_name: str = "workflow.yml",
        key: str = "/mnt/disks/var/bq-dataops-dev-342817.json",
    ):
        self.dbt_manifest_path = dbt_manifest_path
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        self._graph_factory = DbtGraphFactory()
        self.manifest_graph = self._graph_factory.graph()
        self._builder = FlowBuilder(dbt_manifest_path, env, workflow_config_file_name, key)
        self.manifest = self._load_dbt_manifest(self.dbt_manifest_path)
        pass

    def parse_json(self):
        return self._graph_factory.add_execution_tasks(self.manifest)

    def create_tasks(self):
        # TODO (@asledz): Add here calling ta
        pass

    @staticmethod
    def _load_dbt_manifest(manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content
