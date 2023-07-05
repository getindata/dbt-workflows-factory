from __future__ import annotations

from typing import Any

from dbt_graph_builder.builder import (
    GraphConfiguration,
    create_tasks_graph,
    load_dbt_manifest,
)
from dbt_graph_builder.workflow import SequentialStepsGraphFactory

from .params import Params
from .tasks import WorkflowTaskFactory
from .yaml_builder import TaskYamlBuilder


class DbtWorkflowsConverter:
    """Convert dbt manifest to workflows python object representation."""

    def __init__(
        self,
        params: Params,
        manifest_path: str,
    ):
        """Create DbtWorkflowsConverter.

        Args:
            params (Params): Parameters for workflows.
            manifest_path (str): Path to dbt manifest file.
        """
        self._manifest_path = manifest_path
        self._params = params

    def get_yaml(self) -> dict[str, Any]:
        """Convert dbt manifest to workflows python object representation.

        Returns:
            dict[str, Any]: Workflows python object representation.
        """
        dag = create_tasks_graph(load_dbt_manifest(self._manifest_path), GraphConfiguration())
        task_list: list[str] = list(dag.graph.nodes)
        tasks = SequentialStepsGraphFactory(dag, WorkflowTaskFactory()).get_workflow()
        yaml_builder = TaskYamlBuilder(self._params)
        result = yaml_builder.create_workflow(task_list, tasks)
        return result
