from __future__ import annotations

from typing import Any


class Task:
    def __init__(self, job_name: str, params: dict[str, Any]):
        self._job_name = job_name
        self._task_command = params["task_command"]
        self._task_alias = params["task_alias"]

    def create_yml(self):
        return {
            self._task_alias: {
                "call": "subworkflowBatchJob",
                "args": {
                    "batchApiUrl": "${batchApiUrl}",
                    "command": self._task_command,
                    "jobId": self._job_name,
                    "imageUri": "${imageUri}",
                },
                "result": "${" + self._job_name + "}Result",
            }
        }
