from __future__ import annotations

from typing import Any

from dbt_graph_builder.workflow import Step

from .params import Params
from .tasks import CustomStep, WorkflowChainStep


class MainChainTask(WorkflowChainStep):
    """Main chain task in workflow."""

    def __init__(self, step: Step, next_step: WorkflowChainStep | None = None) -> None:
        """Create a new main chain task.

        Args:
            step (Step): The step to add.
            next_step (ChainTask | None, optional): The next step. Defaults to None.
        """
        super().__init__(step, next_step)
        self._step_name = "main"


class TaskYamlBuilder:
    """Task yaml builder."""

    def __init__(self, params: Params):
        """Create a new task yaml builder.

        Args:
            params (Params): GCP workflow params.
        """
        self._params = params

    def create_workflow(
        self,
        additional_steps: Step,
    ) -> dict[str, Any]:
        """Create a workflow.

        Args:
            additional_steps (Step): Additional steps to add to the workflow.

        Returns:
            dict[str, Any]: Workflow.
        """
        maint_chain_task = MainChainTask(self._init_step())
        maint_chain_task.add_step(additional_steps)
        yaml_representation = maint_chain_task.get_step()
        yaml_representation["subworkflowBatchJob"] = self._subworkflow
        return yaml_representation

    def _init_step(self) -> Step:
        return CustomStep(
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
                        {"imageUri": self._params.image_uri},
                    ]
                }
            }
        )

    @property
    def _subworkflow(self) -> dict[str, Any]:
        return {
            "params": self._subworkflow_job_params,
            "steps": self._subworkflow_job_steps,
        }

    @property
    def _subworkflow_job_params(self) -> list[str]:
        return ["batchApiUrl", "command", "jobId", "imageUri", "select"]

    @property
    def _subworkflow_job_steps(self) -> list[dict[str, Any]]:
        return [
            self._subwork_init_job,
            self._subwork_main_job,
            self._subwork_get_job,
        ]

    @property
    def _subwork_init_job(self) -> dict[str, Any]:
        return {
            "init": {
                "assign": [
                    {"jobId": f'${{jobId + "-" + {self._params.job_id_suffix}}}'},
                ]
            }
        }

    @property
    def _subwork_main_job(self) -> dict[str, Any]:
        return {
            "createAndRunBatchJob": {
                "call": "http.post",
                "args": self._subwork_job_args,
                "result": "createAndRunBatchJobResponse",
            }
        }

    @property
    def _subwork_job_args(self) -> dict[str, Any]:
        return {
            "url": "${batchApiUrl}",
            "query": {"jobId": "${jobId}"},
            "headers": {"Content-Type": "application/json"},
            "auth": {"type": "OAuth2"},
            "body": {
                "taskGroups": {
                    "taskSpec": {
                        "volumes": self._subwork_job_volumes,
                        "runnables": self._subwork_job_runnables,
                    }
                },
                "logsPolicy": {"destination": "CLOUD_LOGGING"},
            },
        }

    @property
    def _subwork_job_volumes(self) -> list[dict[str, Any]]:
        return [
            {
                "gcs": {"remotePath": self._params.gcs_key_volume_remote_path},
                "mountPath": self._params.gcs_key_volume_mount_path,
            }
        ]

    @property
    def _subwork_job_runnables(self) -> list[dict[str, Any]]:
        return [
            {
                "container": {
                    "imageUri": "${imageUri}",
                    "entrypoint": "/bin/bash",
                    "commands": [
                        "-c",
                        '${"dbt --no-write-json " + command + " --target env_execution --project-dir /dbt '
                        '--profiles-dir /root/.dbt --select " + select}',
                    ],
                    "volumes": [self._params.gcs_key_volume_container_mount_path],
                },
                "environment": {
                    "variables": {
                        "GCP_KEY_PATH": self._params.container_gcp_key_path,
                        "GCP_PROJECT": self._params.container_gcp_project_id,
                    }
                },
            }
        ]

    @property
    def _subwork_get_job(self) -> dict[str, Any]:
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
