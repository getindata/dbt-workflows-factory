from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbt_graph_builder.workflow import ChainStep, ParallelStep, Step, StepFactory


@dataclass(frozen=True)
class SingleTask(Step):
    task_alias: str
    task_command: str
    job_id: str

    @classmethod
    def from_node(cls, job_id: str, node_def: dict[str, Any]) -> SingleTask:
        return cls(node_def["alias"], node_def["select"], job_id)

    def get_step(self) -> dict[str, Any]:
        return {
            self.task_alias: {
                "call": "subworkflowBatchJob",
                "args": {
                    "batchApiUrl": "${batchApiUrl}",
                    "command": self.task_command,
                    "jobId": self.job_id,
                    "imageUri": "${imageUri}",
                },
                "result": f"${{{self.job_id}}}Result",
            }
        }


class ChainTask(ChainStep):
    chain_counter: int = 0

    def __init__(self, step: Step, next_step: ChainStep | None = None) -> None:
        super().__init__(step, next_step)
        self._task_alias = f"branch-{self.chain_counter}"
        ChainTask.chain_counter += 1

    def get_step(self) -> dict[str, Any]:
        if self._next_step is None:
            return {self._task_alias: {"steps": [self._step.get_step()]}}
        return {
            self._task_alias: {
                "steps": [self._step.get_step(), *(next(iter(self._next_step.get_step().values()))["steps"])]
            }
        }


class ParallelTask(ParallelStep):
    parallel_counter: int = 0

    def __init__(self, steps: list[Step]) -> None:
        super().__init__(steps)
        self._task_alias = f"parallel-{self.parallel_counter}"
        ParallelTask.parallel_counter += 1

    def get_step(self) -> dict[str, Any]:
        return {self._task_alias: {"parallel": {"branches": [step.get_step() for step in self._steps]}}}


class WorkflowTaskFactory(StepFactory):
    def create_single_step(self, node: str, node_definition: dict[str, Any]) -> SingleTask:
        return SingleTask.from_node(node, node_definition)

    def create_chain_step(self, task: Step) -> ChainTask:
        return ChainTask(task)

    def create_parallel_step(self) -> ParallelTask:
        return ParallelTask([])
