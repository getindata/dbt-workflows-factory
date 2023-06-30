from __future__ import annotations

import yaml

from .dag_factory.dag_factory import DbtManifestParser
from .flow_builder import FlowBuilder
from .params import Params
from .yaml_builder import TaskYamlBuilder


class DbtWorkflowsConverter:
    def __init__(
        self,
        params: Params,
        manifest_path: str,
        workflows_path: str,
    ):
        self._manifest_path = manifest_path
        self._workflows_path = workflows_path
        self._params = params
        self._parser = DbtManifestParser()

    def get_yaml(self):
        dag = self._parser.parse_manifest(manifest_file_path=self._manifest_path)
        yaml_builder = TaskYamlBuilder(self._params)
        flow_builder = FlowBuilder(dag)

        task_list = flow_builder.create_task_list()
        task_structure = flow_builder.create_task_structure()
        tasks_yaml = yaml_builder.create_tasks_yaml_list(task_structure)
        result = yaml_builder.create_workflow(task_list, tasks_yaml)

        return result
