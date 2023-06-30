from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


class Task(ABC):
    @abstractmethod
    def create_yml(self) -> dict[str, Any]:
        """Create yaml representation for a task.

        Returns:
            dict[str, Any]: YAML task representation.
        """


@dataclass
class SingleTask(Task):
    job_id: str
    task_command: str
    task_alias: str

    @classmethod
    def from_node(cls, job_id: str, node_def: dict[str, Any]) -> Task:
        return cls(job_id, node_def["select"], node_def["alias"])

    def create_yml(self) -> dict[str, Any]:
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


@dataclass
class TaskList(Task):
    job_id: str
    tasks: list[SingleTask]

    def create_yml(self) -> dict[str, Any]:
        return
