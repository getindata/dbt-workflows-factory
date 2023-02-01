from flow_builder import FlowBuilder
from dag_factory.dbt_graph_factory import DbtGraphFactory
import json
import logging
class DbtWorkflowsConverter:
    dag_path: str
    env: str
    workflow_config_file_name: str
    _builder: FlowBuilder

    def __init__(
            self,
            manifest_path: str,
            env: str,
            workflow_config_file_name: str = "workflow.yml",
            key: str = "/mnt/disks/var/bq-dataops-dev-342817.json",
    ):
        self.manifest_path = manifest_path
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        self._builder = FlowBuilder(manifest_path, env,
                                    workflow_config_file_name, key)
        self._graph_factory = DbtGraphFactory()
        pass

    def parse_json(self):
        manifest = self._load_dbt_manifest(self.manifest_path)
        return self._graph_factory.add_execution_tasks(manifest)

    def create_tasks(self):
        # TODO (@asledz): Add here calling ta
        pass

    @staticmethod
    def _load_dbt_manifest(manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content