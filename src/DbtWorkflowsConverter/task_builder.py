from src.DbtWorkflowsConverter.dag_factory.dbt_graph_factory import DbtGraphFactory
from src.DbtWorkflowsConverter.utils.node_type import NodeType
import json
import logging
from typing import Any, ContextManager, Dict, Tuple
import airflow

if not airflow.__version__.startswith("1."):
    from airflow.utils.task_group import TaskGroup


class TaskBuilder:
    def __init__(self, manifest_path: str, env: str, workflow_config_file_name: str, key: str):
        self.key = key
        self.manifest_path = manifest_path
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        pass

    def parse_manifest_into_tasks(self, manifest_path: str) -> Dict[Any, Any]:
        """
        Parse ``manifest.json`` into tasks.
        :param manifest_path: Path to ``manifest.json``.
        :type manifest_path: str
        :return: Dictionary of tasks created from ``manifest.json`` parsing.
        :rtype: ModelExecutionTasks
        """
        return self._make_dbt_tasks(manifest_path)

    @staticmethod
    def _load_dbt_manifest(manifest_path: str) -> dict:
        with open(manifest_path, "r") as f:
            manifest_content = json.load(f)
            logging.debug("Manifest content: " + str(manifest_content))
            return manifest_content

    def _create_tasks_graph(self, manifest: dict) -> None:
        self.graph = DbtGraphFactory()
        self.graph.add_execution_tasks(manifest)
        self.graph.contract_test_nodes()

    def _make_dbt_tasks(self, manifest_path: str) -> Dict[Any, Any]:
        manifest = self._load_dbt_manifest(manifest_path)
        self._create_tasks_graph(manifest)
        tasks_with_context = self._create_tasks_from_graph(self.graph)

        return tasks_with_context

    def _create_tasks_from_graph(self, dbt_airflow_graph: DbtGraphFactory) -> Dict[Any, Any]:

        result_tasks = {
            node_name: self._create_task_from_graph_node(node_name, node)
            for node_name, node in dbt_airflow_graph.graph.nodes(data=True)
        }
        return result_tasks

    def _create_task_from_graph_node(
            self, node_name: str, node: Dict[str, Any]
    ) -> dict[Any, Any]:
        if node["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
            pass
        elif node["node_type"] == NodeType.SOURCE_SENSOR:
            pass
        elif node["node_type"] == NodeType.MOCK_GATEWAY:
            pass
        else:
            return self._create_task_for_model(
                node["select"],
                node["node_type"] == NodeType.EPHEMERAL,
                self.airflow_config.use_task_group,
            )

    def _create_task_for_model(
            self,
            model_name: str,
            is_ephemeral_task: bool,
            use_task_group: bool,
    ) -> dict[Any, Any]:

        (task_group, task_group_ctx) = self._create_task_group_for_model(model_name, use_task_group)
        is_in_task_group = task_group is not None
        run_task = self._make_dbt_run_task(model_name, is_in_task_group)
        return run_task

    @staticmethod
    def _create_task_group_for_model(
            model_name: str, use_task_group: bool
    ) -> Tuple[Any, ContextManager]:
        import contextlib

        is_first_version = airflow.__version__.startswith("1.")
        task_group = (
            None if (is_first_version or not use_task_group) else TaskGroup(group_id=model_name)
        )
        task_group_ctx = task_group or contextlib.nullcontext()
        return task_group, task_group_ctx

    @staticmethod
    def _build_task_name(model_name: str, command: str, is_in_task_group: bool) -> str:
        return command if is_in_task_group else f"{model_name}_{command}"

    def _make_dbt_run_task(self, model_name: str, is_in_task_group: bool) -> dict[Any, Any]:
        command = "run"
        name = self._build_task_name(model_name, command, is_in_task_group)
        return self.create_singular_task(name, command, name + "_job")

    @staticmethod
    def create_singular_task(job_name: str, command: str, job_id: str):
        job_id = "${" + job_id + "}"
        result = {
            job_name: {
                "call": "subworkflowBatchJob",
                "args": {
                    "batchApiUrl": "${batchApiUrl}",
                    "command": command,
                    "jobId": job_id,
                    "imageUri": "${imageUri}",
                },
                "result": job_id + "Result",
            }
        }
        return result
