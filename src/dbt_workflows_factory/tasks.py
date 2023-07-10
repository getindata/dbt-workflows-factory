from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from dbt_graph_builder.node_type import NodeType
from dbt_graph_builder.workflow import ChainStep, ParallelStep, Step, StepFactory


class DbtCommand(Enum):
    """Dbt command."""

    RUN = "run"
    TEST = "test"


class CustomStep(Step):
    """Simple single task in workflow."""

    def __init__(self, get_step_def: dict[str, Any]) -> None:
        """Create a new simple single task.

        Args:
            get_step_def (dict[str, Any]): The step definition.
        """
        self._get_step_def = get_step_def

    def get_step(self) -> dict[str, Any]:
        """Return a step result.

        Returns:
            dict[str, Any]: Step result.
        """
        return self._get_step_def


@dataclass(frozen=True)
class NodeStep(Step):
    """Single task in workflow."""

    step_name: str
    select: str
    command: DbtCommand
    job_id: str

    def get_step(self) -> dict[str, Any]:
        """Return a step result.

        Returns:
            dict[str, Any]: Step result.
        """
        return {
            self.step_name: {
                "call": "subworkflowBatchJob",
                "args": {
                    "jobId": self.job_id,
                    "select": self.select,
                    "command": self.command.value,
                },
                "result": f"{self.step_name}_RESULT",
            }
        }


class WorkflowChainStep(ChainStep):
    """Chain task in workflow."""

    chain_counter: int = 0

    def __init__(self, step: Step, next_step: ChainStep | None = None) -> None:
        """Create a new chain task.

        Args:
            step (Step): The step to add.
            next_step (ChainStep | None, optional): The next step. Defaults to None.
        """
        super().__init__(step, next_step)
        self._step_name = f"chain_{self.chain_counter}"
        WorkflowChainStep.chain_counter += 1

    def get_step(self) -> dict[str, Any]:
        """Return a step result.

        Returns:
            dict[str, Any]: Step result.
        """
        if self._next_step is None:
            return {self._step_name: {"steps": [self._step.get_step()]}}

        return {
            self._step_name: {
                "steps": [self._step.get_step(), *(next(iter(self._next_step.get_step().values()))["steps"])]
            }
        }


class WorkflowParallelStep(ParallelStep):
    """Parallel task in workflow."""

    parallel_counter: int = 0

    def __init__(self, steps: list[Step]) -> None:
        """Create a new parallel task.

        Args:
            steps (list[Step]): The parallel steps.
        """
        super().__init__(steps)
        self._step_name = f"parallel_{self.parallel_counter}"
        WorkflowParallelStep.parallel_counter += 1

    def get_step(self) -> dict[str, Any]:
        """Return a step result.

        Returns:
            dict[str, Any]: Step result.
        """
        return {self._step_name: {"parallel": {"branches": [step.get_step() for step in self._steps]}}}


class WorkflowStepFactory(StepFactory):
    """Workflow task factory."""

    _job_id_replace_pattern = re.compile(r"[^a-zA-Z0-9\-]")
    _task_name_replace_pattern = re.compile(r"[^a-zA-Z0-9_]")

    def create_node_step(self, node: str, node_definition: dict[str, Any]) -> Step:
        """Create a single step.

        Args:
            node (str): Node name.
            node_definition (dict[str, Any]): Node definition.

        Raises:
            NotImplementedError: Unsupported node type.

        Returns:
            SingleTask: SingleTask instance.
        """
        node_id: str = self._job_id_replace_pattern.sub("-", node).lower()[0:36]
        job_id = f"{node_id}-{hashlib.sha256(node.encode('utf-8')).hexdigest()[0:8].lower()}"
        task_name: str = self._task_name_replace_pattern.sub("_", node)

        if node_definition["node_type"] == NodeType.RUN_TEST:
            run_task = NodeStep(f"{task_name}_run", node_definition["select"], DbtCommand.RUN, f"{job_id}-run")
            test_task = NodeStep(f"{task_name}_test", node_definition["select"], DbtCommand.TEST, f"{job_id}-test")
            return WorkflowChainStep(run_task, WorkflowChainStep(test_task))

        if node_definition["node_type"] == NodeType.MULTIPLE_DEPS_TEST:
            return NodeStep(task_name, node_definition["select"], DbtCommand.TEST, job_id)

        if node_definition["node_type"] == NodeType.EPHEMERAL:
            return CustomStep(
                {
                    "call": "sys.log",
                    "args": {"text": f"Skipping ephemeral node: {node_definition['alias']}", "severity": "INFO"},
                }
            )

        raise NotImplementedError(f"Unsupported node type: {node_definition['node_type']}")

    def create_chain_step(self, task: Step) -> WorkflowChainStep:
        """Create a chain step.

        Args:
            task (Step): Task to add.

        Returns:
            ChainTask: ChainTask instance.
        """
        return WorkflowChainStep(task)

    def create_parallel_step(self) -> WorkflowParallelStep:
        """Create a parallel step.

        Returns:
            ParallelTask: ParallelTask instance.
        """
        return WorkflowParallelStep([])
