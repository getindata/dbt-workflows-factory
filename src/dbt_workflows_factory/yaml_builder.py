from __future__ import annotations

from typing import Any

from dbt_graph_builder.workflow import ChainStep, Step

from .params import Params


class MainChainTask(ChainStep):
    def get_step(self) -> dict[str, Any]:
        if self._next_step is None:
            return {"main": {"steps": [self._step.get_step()]}}
        return {"main": {"steps": [self._step.get_step(), *(next(iter(self._next_step.get_step().values()))["steps"])]}}


class SimpleSingleTask(Step):
    def __init__(self, get_step_def: dict[str, Any]) -> None:
        self._get_step_def = get_step_def

    def get_step(self) -> dict[str, Any]:
        return self._get_step_def


class TaskYamlBuilder:
    def __init__(self, params: Params):
        self._params = params

    @property
    def subwork_batch_job_init(self) -> dict[str, Any]:
        return {"init": {"assign": [{"fullcomand": self._params.full_command}]}}

    @property
    def subwork_batch_job_main(self) -> dict[str, Any]:
        return {
            "createAndRunBatchJob": {
                "call": "http.post",
                "args": self.subwork_batch_job_main_args,
                "result": "createAndRunBatchJobResponse",
            }
        }

    @property
    def subwork_batch_job_main_args(self) -> dict[str, Any]:
        return {
            "url": "${batchApiUrl}",
            "query": {"job_id": "${jobId}"},
            "headers": {"Content-Type": "application/json"},
            "auth": {"type": "OAuth2"},
            "body": {
                "taskGroups": {
                    "taskSpec": {
                        "volumes": self.subwork_batch_job_main_args_volumes,
                        "runnables": self.subwork_batch_job_main_args_runnables,
                    }
                },
                "logsPolicy": {"destination": "CLOUD_LOGGING"},
            },
        }

    @property
    def subwork_batch_job_main_args_runnables(self) -> list[dict[str, Any]]:
        return [
            {
                "container": {
                    "imageUri": "imageUri",
                    "entrypoint": "bash",
                    "commands": ["-c", "full_command"],
                    "volumes": [self._params.key_volume_path],
                },
                "environment": {
                    "variables": {
                        "GCP_KEY_PATH": self._params.key_path,
                        "GCP_PROJECT": "dataops" "-test" "-project",
                    }
                },
            }
        ]

    @property
    def subwork_batch_job_main_args_volumes(self) -> list[dict[str, Any]]:
        return [
            {
                "gcs": {"remotePath": self._params.remote_path},
                "mountPath": self._params.key_volume_mount_path,
            }
        ]

    @property
    def subwork_batch_job_get(self) -> dict[str, Any]:
        return {
            "getJob": {
                "call": "http.get",
                "args": {
                    "url": '${batchApiUrl + "/" + jobId}',
                    "auth": {"type": "OAuth2"},
                },
                "result": "getJobResult",
            }
        }

    @property
    def subworkflow(self) -> dict[str, Any]:
        return {
            "params": self.subworkflow_batch_job_params,
            "steps": self.subworkflow_batch_job_steps,
        }

    @property
    def subworkflow_batch_job_steps(self) -> list[dict[str, Any]]:
        return [
            self.subwork_batch_job_init,
            self.subwork_batch_job_main,
            self.subwork_batch_job_get,
        ]

    @property
    def subworkflow_batch_job_params(self) -> list[str]:
        return ["batchApiUrl", "command", "jobId", "imageUri"]

    def init_step(self, job_names: list[str]) -> Step:
        return SimpleSingleTask(
            {
                "init": {
                    "assign": [
                        {"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'},
                        {"region": self._params.region},
                        {"batchApi": "batch.googleapis.com/v1"},
                        {
                            "batchApiUrl": (
                                '${"https://" + batchApi + "/projects/" + projectId + "/locations/" + region + "/jobs"}'
                            )
                        },
                        {"containerEntrypoint": "bash"},
                        {"imageUri": self._params.image_uri},
                        *[{task_name: f"${{{task_name.lower()} + string(int(sys.now()))}}"} for task_name in job_names],
                    ]
                }
            }
        )

    def create_workflow(
        self,
        job_list: list[str],
        additional_steps: Step,
    ) -> dict[str, Any]:
        maint_chain_task = MainChainTask(self.init_step(job_list))
        maint_chain_task.add_step(additional_steps)
        yaml_representation = maint_chain_task.get_step()
        yaml_representation["subworkflowBatchJob"] = self.subworkflow
        return yaml_representation
