from src.DbtWorkflowsConverter.task_builder import TaskBuilder
from src.DbtWorkflowsConverter.dbt_workflows_converter_params import DbtWorkflowsConverterParams
from src.DbtWorkflowsConverter.workflow_parameters import WorkflowParameters

class FlowBuilder:
    def __init__(
        self,
        params: DbtWorkflowsConverterParams,
        workflow_parameters: WorkflowParameters
    ):
        self.key = params.get_key()
        self.dbt_manifest_path = params.get_key()
        self.env = params.get_key()
        self.workflow_config_file_name = params.get_key()
        self._builder = TaskBuilder(self.dbt_manifest_path, self.env, self.workflow_config_file_name, self.key)
        self.params = workflow_parameters if workflow_parameters is not None else WorkflowParameters()

        command = self.params.get_command()

        key_volume_path = self.params.get_key_volume_path()

        key_volume_mount_path = self.params.get_key_volume_mount_path()

        remote_path = self.params.get_key_remote_path()
        region = self.params.get_region() 

        self.workflow_init = {
            "init": {
                "assign": [
                    {"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'},
                    {"region": region},
                    {"batchApi": "batch.googleapis.com/v1"},
                    {
                        "batchApiUrl": '${"https://" + batchApi + "/projects/" + projectId + '
                        '"/locations/" + '
                        'region + "/jobs"}'
                    },
                    {"containerEntrypoint": "bash"},
                ]
            }
        }
        self.subworkflow_yaml = {
            "subworkflowBatchJob": {
                "params": ["batchApiUrl", "command", "jobId", "imageUri"],
                "steps": [
                    {"init": {"assign": [{"fullcomand": command}]}},
                    {
                        "createAndRunBatchJob": {
                            "call": "http.post",
                            "args": {
                                "url": "${batchApiUrl}",
                                "query": {"job_id": "${jobId}"},
                                "headers": {"Content-Type": "application/json"},
                                "auth": {"type": "OAuth2"},
                                "body": {
                                    "taskGroups": {
                                        "taskSpec": {
                                            "volumes": [
                                                {"gcs": {"remotePath": remote_path}, "mountPath": key_volume_mount_path}
                                            ],
                                            "runnables": [
                                                {
                                                    "container": {
                                                        "imageUri": "imageUri",
                                                        "entrypoint": "bash",
                                                        "commands": ["-c", "full_command"],
                                                        "volumes": [key_volume_path],
                                                    },
                                                    "environment": {
                                                        "variables": {
                                                            "GCP_KEY_PATH": "keypath",
                                                            "GCP_PROJECT": "dataops" "-test" "-project",
                                                        }
                                                    },
                                                }
                                            ],
                                        }
                                    },
                                    "logsPolicy": {"destination": "CLOUD_LOGGING"},
                                },
                            },
                            "result": "createAndRunBatchJobResponse",
                        }
                    },
                    {
                        "getJob": {
                            "call": "http.get",
                            "args": {"url": '${batchApiUrl + "/" + jobId}', "auth": {"type": "OAuth2"}},
                            "result": "getJobResult",
                        }
                    },
                ],
            }
        }

    def create_init(self, job_names: list[str], image_uri: str):
        workflow_init = self.workflow_init
        workflow_init["init"]["assign"].append({"imageUri": image_uri})
        if job_names is not None:
            for task in job_names:
                taskid = '${"' + task.lower() + '" + string(int(sys.now()))}'
                workflow_init["init"]["assign"].append({task: taskid})
        return workflow_init

    def create_subworkflow(self):
        subworkflow = self.subworkflow_yaml
        subworkflow["subworkflowBatchJob"]["steps"][1]["createAndRunBatchJob"]["args"]["body"]["taskGroups"][
            "taskSpec"
        ]["runnables"][0]["environment"]["variables"]["GCP_KEY_PATH"] = self.key
        return subworkflow

    def create_mainflow(self, job_names: list[str] or None, image_uri: str):
        init = self.create_init(job_names, image_uri)
        res = {"main": {"steps": []}}
        res["main"]["steps"].append(init)
        tasks = self._builder.parse_manifest_into_tasks(self.dbt_manifest_path)
        res["main"]["steps"].append(tasks)
        return res

    def create_workflow(self, image_uri: str):
        flow_yaml_desc = {
            "main": self.create_mainflow(None, image_uri)["main"],
            "subworkflowBatchJob": self.create_subworkflow()["subworkflowBatchJob"],
        }
        return flow_yaml_desc
