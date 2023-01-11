from task_builder import TaskBuilder


class FlowBuilder:

    def __init__(self, manifest_path: str, env: str,
                 workflow_config_file_name: str, key: str):
        self.key = key
        self.dag_path = manifest_path
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        self._builder = TaskBuilder(manifest_path, env,
                                    workflow_config_file_name, key)
        self.workflow_init = {'init': {
            'assign': [
                {'projectId': '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'},
                {'region': 'europe-west6'},
                {'batchApi': 'batch.googleapis.com/v1'},
                {'batchApiUrl':
                     '${"https://" + batchApi + "/projects/" + projectId + '
                     '"/locations/" + '
                     'region + "/jobs"}'},
                {'containerEntrypoint': 'bash'}]}}
        self.subworkflow_yaml = {
            'subworkflowBatchJob':
                {'params': ['batchApiUrl', 'command', 'jobId', 'imageUri'],
                 'steps': [{'init': {'assign': [
                     {'fullcomand': '${"dbt --no-write-json run --target '
                                    'env_execution '
                                    '--project-dir /dbt --profiles-dir '
                                    '/root/.dbt '
                                    '--select " + command }'}]}}, {
                     'createAndRunBatchJob':
                         {'call': 'http.post',
                          'args': {
                              'url': '${batchApiUrl}',
                              'query': {
                                  'job_id': '${jobId}'},
                              'headers': {
                                  'Content-Type': 'application/json'},
                              'auth': {
                                  'type': 'OAuth2'},
                              'body': {'taskGroups': {
                                  'taskSpec': {
                                      'volumes': [{
                                          'gcs': {
                                              'remotePath': 'dataops-dev'
                                                            '-state'},
                                          'mountPath': '/mnt/disks/var'}],
                                      'runnables': [{
                                          'container': {
                                              'imageUri': 'imageUri',
                                              'entrypoint': 'bash',
                                              'commands': [
                                                  '-c',
                                                  'full_command'],
                                              'volumes': [
                                                  '/mnt/disks/var/:/mnt'
                                                  '/disks/var/:rw']},
                                          'environment': {
                                              'variables': {
                                                  'GCP_KEY_PATH': 'keypath',
                                                  'GCP_PROJECT': 'dataops'
                                                                 '-test'
                                                                 '-project'
                                              }
                                          }
                                      }]
                                  }
                              },
                                  'logsPolicy': {
                                      'destination': 'CLOUD_LOGGING'}}},
                          'result': 'createAndRunBatchJobResponse'}},
                     {'getJob': {'call': 'http.get',
                                 'args': {
                                     'url': '${batchApiUrl + "/" + jobId}',
                                     'auth': {'type': 'OAuth2'}},
                                 'result': 'getJobResult'}}]}}

    def create_init(self, job_names: list[str], image_uri: str):
        workflow_init = self.workflow_init
        workflow_init['init']['assign'].append({'imageUri': image_uri})
        for task in job_names:
            taskid = '${"' + task.lower() + '" + string(int(sys.now()))}'
            workflow_init['init']['assign'].append({task: taskid})
        return workflow_init

    def create_subworkflow(self):
        subworkflow = self.subworkflow_yaml
        subworkflow['subworkflowBatchJob']['steps'][1]['createAndRunBatchJob'][
            'args']['body'][
            'taskGroups'][
            'taskSpec']['runnables'][0]['environment']['variables'][
            'GCP_KEY_PATH'] = self.key
        return subworkflow

    def create_mainflow(self, job_names: list[str], job_commands: list[str],
                        image_uri: str):
        init = self.create_init(job_names, image_uri)
        res = {'main': {'steps': []}}
        res['main']['steps'].append(init)
        for job_id, command in zip(job_names, job_commands):
            print("kupa")
            job_name = job_id + "Job"
            task_yaml = self._builder.create_task(job_name, command, job_id)
            res['main']['steps'].append(task_yaml)
        return res

    def create_workflow(self, job_names: list[str], job_commands: list[str],
                        image_uri: str):
        flow_yaml_desc = {
            'main': self.create_mainflow(job_names, job_commands, image_uri)[
                'main'],
            'subworkflowBatchJob': self.create_subworkflow()[
                'subworkflowBatchJob'], }
        return flow_yaml_desc
