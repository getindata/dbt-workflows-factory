class TaskBuilder:
    def __init__(self, manifest_path: str, env: str,
                 workflow_config_file_name: str, key: str):
        self.key = key
        self.dag_path = manifest_path
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        pass

    def create_task(self, job_name: str, command: str, job_id: str):
        job_id = '${' + job_id + '}'
        result = {job_name: {'call': 'subworkflowBatchJob',
                             'args': {'batchApiUrl': '${batchApiUrl}',
                                      'command': command,
                                      'jobId': job_id,
                                      'imageUri': '${imageUri}'},
                             'result': job_id + 'Result'}}
        return result
