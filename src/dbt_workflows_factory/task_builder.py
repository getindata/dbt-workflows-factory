from __future__ import annotations

from typing import Any


class Task:
    def __init__(self, job_name: str, node_def: dict[str, Any]):
        self._job_id = job_name
        self._task_command = node_def["select"]
        self._task_alias = node_def["alias"]

    def __repr__(self) -> str:
        return f'Task(job_name="{self.job_id}", node_def={{"select": "{self._task_command}", "alias": "{self._task_alias}"}})'

    @property
    def job_id(self) -> str:
        return self._job_id

    def create_yml(self):
        return {
            self._task_alias: {
                "call": "subworkflowBatchJob",
                "args": {
                    "batchApiUrl": "${batchApiUrl}",
                    "command": self._task_command,
                    "jobId": self.job_id,
                    "imageUri": "${imageUri}",
                },
                "result": f"${{{self.job_id}}}Result",
            }
        }
