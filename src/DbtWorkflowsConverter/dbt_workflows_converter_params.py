class DbtWorkflowsConverterParams:
    def __init__(
        self,
        dbt_manifest_path: str,
        env: str,
        workflow_config_file_name: str = "workflow.yml",
        key: str = "/mnt/disks/var/bq-dataops-dev-342817.json",
    ):
        self.dbt_manifest_path = dbt_manifest_path
        self.env = env
        self.workflow_config_file_name = workflow_config_file_name
        self.key = key

    def get_dbt_manifest_path(self):
        return self.dbt_manifest_path

    def get_env(self):
        return self.env

    def get_workflow_config_file_name(self):
        return self.workflow_config_file_name

    def get_key(self):
        return self.key
