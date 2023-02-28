class TaskBuilder:
    def __init__(self, manifest: dict, env: str, workflow_config_file_name: str, key: str):
        self.key = key
        self.manifest = manifest
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        pass

    def create_task(self, job_name: str, command: str, job_id: str):
        job_id = "${" + job_id + "}"
        result = {
            job_name: {
                "call": "subworkflowBatchJob",
                "args": {
                    "batchApiUrl": "${batchApiUrl}",
                    "command": command,
                    "jobId": job_id,
                    "imageUri": "${imageUri}",
                },
                "result": job_id + "Result",
            }
        }
        return result
