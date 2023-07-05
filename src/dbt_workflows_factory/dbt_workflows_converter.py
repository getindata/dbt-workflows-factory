from __future__ import annotations

from typing import Any

from dbt_graph_builder.builder import (
    GraphConfiguration,
    create_tasks_graph,
    load_dbt_manifest,
)
from dbt_graph_builder.workflow import SequentialStepsGraphFactory

from .params import Params
from .task import WorkflowTaskFactory
from .yaml_builder import TaskYamlBuilder


class DbtWorkflowsConverter:
    def __init__(
        self,
        params: Params,
        manifest_path: str,
    ):
        self._manifest_path = manifest_path
        self._params = params

    def get_yaml(self) -> dict[str, Any]:
        dag = create_tasks_graph(load_dbt_manifest(self._manifest_path), GraphConfiguration())
        task_list: list[str] = list(dag.graph.nodes)
        tasks = SequentialStepsGraphFactory(dag, WorkflowTaskFactory()).get_workflow()
        yaml_builder = TaskYamlBuilder(self._params)
        result = yaml_builder.create_workflow(task_list, tasks)
        return result
