class Task:
    def __init__(self, job_name: str, command: str, job_id: str):
        self.job_id = job_id
        self.command = command
        self.job_name = job_name

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
