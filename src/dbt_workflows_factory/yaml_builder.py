from params import Params
from task_builder import Task


class YamlLib:
    def __init__(self, params: Params):
        self.params = params
        self.branch_number = 0

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
                    {"imageUri": self.params.image_uri},
                ]
            }
        }
        return workflow_init

    def subworkflow_definition_yaml(self):
        return {
            "subworkflowBatchJob": {
                "params": ["batchApiUrl", "command", "jobId", "imageUri"],
                "steps": [
                    {
                        "init": {
                            "assign": [{"fullcomand": self.params.full_command}]
                        }
                    },
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
                                                    "gcs": {
                                                        "remotePath": self.params.remote_path
                                                    },
                                                    "mountPath": self.params.key_volume_mount_path,
                                                }
                                            ],
                                            "runnables": [
                                                {
                                                    "container": {
                                                        "imageUri": "imageUri",
                                                        "entrypoint": "bash",
                                                        "commands": [
                                                            "-c",
                                                            "full_command",
                                                        ],
                                                        "volumes": [
                                                            self.params.key_volume_path
                                                        ],
                                                    },
                                                    "environment": {
                                                        "variables": {
                                                            "GCP_KEY_PATH": self.params.key_path,
                                                            "GCP_PROJECT": "dataops"
                                                            "-test"
                                                            "-project",
                                                        }
                                                    },
                                                }
                                            ],
                                        }
                                    },
                                    "logsPolicy": {
                                        "destination": "CLOUD_LOGGING"
                                    },
                                },
                            },
                            "result": "createAndRunBatchJobResponse",
                        }
                    },
                    {
                        "getJob": {
                            "call": "http.get",
                            "args": {
                                "url": '${batchApiUrl + "/" + jobId}',
                                "auth": {"type": "OAuth2"},
                            },
                            "result": "getJobResult",
                        }
                    },
                ],
            }
        }

    def create_init(self, job_names: "list[str]"):
        workflow_init = self.init_workflow()
        if job_names is not None:
            for task in job_names:
                taskid = '${"' + task.lower() + '" + string(int(sys.now()))}'
                workflow_init["init"]["assign"].append({task: taskid})
        return workflow_init

    def subworkflow(self):
        return self.subworkflow_definition_yaml()

    def create_mainflow(self, job_names: "list[str]", tasks):
        init = self.create_init(job_names)
        res = {"main": {"steps": []}}
        res["main"]["steps"].append(init)
        res["main"]["steps"].append(tasks)
        return res

    def create_tasks_yaml_list(self, tasks_structure):
        result = {}
        for task_branch in tasks_structure:
            # Singular task
            if isinstance(task_branch, list):
                parallel_structure = {"parallel": {"branches": []}}
                for branch in task_branch:
                    self.branch_number += 1
                    branch_name = "branch" + str(self.branch_number)
                    tasks = self.create_tasks_yaml_list(branch)
                    parallel_structure["parallel"]["branches"].append(
                        {branch_name: {"steps": tasks}}
                    )
                pass
            elif isinstance(task_branch, Task):
                result.update(task_branch.create_yml())
                pass
            else:
                raise Exception("An unexpected type occurred")
            pass

        return result

    def create_workflow(self, tasks, job_list):
        flow_yaml_desc = {
            "main": self.create_mainflow(job_list, tasks)["main"],
            "subworkflowBatchJob": self.subworkflow()["subworkflowBatchJob"],
        }
        return flow_yaml_desc
