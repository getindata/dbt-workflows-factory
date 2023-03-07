import json
import logging
import yaml

from src.DbtWorkflowsConverter.dag_factory.dbt_graph_factory import DbtGraphFactory
from src.DbtWorkflowsConverter.flow_builder import FlowBuilder
from src.DbtWorkflowsConverter.dbt_workflows_converter_params import DbtWorkflowsConverterParams


class DbtWorkflowsConverter:
    _builder: FlowBuilder

    def __init__(
        self,
        params: DbtWorkflowsConverterParams,
        workflows_yaml_name: str = "workflow.yaml"
    ):
        self.params = params
        self._graph_factory = DbtGraphFactory()
        self._builder = FlowBuilder(
            self.params,
        )
        self.manifest_path = self.params.get_dbt_manifest_path()
        self.workflows_yaml_name = workflows_yaml_name
        self.flow_json = None

    def create_tasks(self):
        self.flow_json = self._builder.create_workflow(self.manifest_path)
        return self.flow_json

    def write_tasks(self):
        if self.flow_json is None:
            self.create_tasks()
        with open(self.workflows_yaml_name, 'w') as outfile:
            yaml.dump(self.flow_json, outfile, default_flow_style=False)


    @staticmethod
    def _load_dbt_manifest(manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content
