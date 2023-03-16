from dbt_workflows_converter.params import Params
from dbt_workflows_converter.task_builder import Task


class YamlLib:
    def __init__(self, params: Params):
        self.params = params

    def init_workflow(self):
        workflow_init = {
            "init": {
                "assign": [
                    {"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'},
                    {"region": self.params.region},
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
        return workflow_init

    def subworkflow_definition_yaml(self):
        return {
            "subworkflowBatchJob": {
                "params": ["batchApiUrl", "command", "jobId", "imageUri"],
                "steps": [
                    {"init": {"assign": [{"fullcomand": self.params.full_command}]}},
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
                                                {
                                                    "gcs": {"remotePath": self.params.remote_path},
                                                    "mountPath": self.params.key_volume_mount_path,
                                                }
                                            ],
                                            "runnables": [
                                                {
                                                    "container": {
                                                        "imageUri": "imageUri",
                                                        "entrypoint": "bash",
                                                        "commands": ["-c", "full_command"],
                                                        "volumes": [self.params.key_volume_path],
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

    def create_init(self, job_names: "list[str]"):
        workflow_init = self.init_workflow()
        workflow_init["init"]["assign"].append({"imageUri": self.params.image_uri})
        if job_names is not None:
            for task in job_names:
                taskid = '${"' + task.lower() + '" + string(int(sys.now()))}'
                workflow_init["init"]["assign"].append({task: taskid})
        return workflow_init

    def subworkflow(self):
        subworkflow = self.subworkflow_definition_yaml()
        subworkflow["subworkflowBatchJob"]["steps"][1]["createAndRunBatchJob"]["args"]["body"]["taskGroups"][
            "taskSpec"
        ]["runnables"][0]["environment"]["variables"]["GCP_KEY_PATH"] = self.params.key_path
        return subworkflow

    def create_mainflow(self, job_names: "list[str]", tasks):
        init = self.create_init(job_names)
        res = {"main": {"steps": []}}
        res["main"]["steps"].append(init)
        res["main"]["steps"].append(tasks)
        return res

    def create_tasks_yaml_list(self, tasks: "list[Task]"):
        pass

    def create_workflow(self, tasks, job_list):
        flow_yaml_desc = {
            "main": self.create_mainflow(job_list, tasks)["main"],
            "subworkflowBatchJob": self.subworkflow()["subworkflowBatchJob"],
        }
        return flow_yaml_desc
