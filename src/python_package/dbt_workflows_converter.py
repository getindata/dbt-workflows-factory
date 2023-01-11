from flow_builder import FlowBuilder


class DbtWorkflowsConverter:
    dag_path: str
    env: str
    workflow_config_file_name: str
    _builder: FlowBuilder

    def __init__(
            self,
            manifest_path: str,
            env: str,
            workflow_config_file_name: str = "workflow.yml",
            key: str = "/mnt/disks/var/bq-dataops-dev-342817.json",
    ):
        self.dag_path = manifest_path
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        self._builder = FlowBuilder(manifest_path, env,
                                    workflow_config_file_name, key)
        pass

    def parse_json(self):
        pass

    def create_tasks(self):
        # TODO (@asledz): Add here calling ta
        pass
