class Task:
    def __init__(self, job_name: str, params: "dict"):
        self.job_id = job_name
        self.command = params["task_command"]
        self.job_name = params["task_alias"]

    def create_yml(self):
        job_id = "${" + self.job_id + "}"
        result = {
            self.job_name: {
                "call": "subworkflowBatchJob",
                "args": {
                    "batchApiUrl": "${batchApiUrl}",
                    "command": self.command,
                    "jobId": self.job_id,
                    "imageUri": "${imageUri}",
                },
                "result": job_id + "Result",
            }
        }
        return result
