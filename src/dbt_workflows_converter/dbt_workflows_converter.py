import yaml
from dbt_workflows_converter.dag_factory.dag_factory import DbtManifestParser
from dbt_workflows_converter.params import Params
from dbt_workflows_converter.yaml_builder import YamlLib
from dbt_workflows_converter.flow_builder import FlowBuilder

class DbtWorkflowsConverter:
    def __init__(
        self, params: Params, manifest_path: str = "manifest.json", workflows_yaml_name: str = "workflow.yaml"
    ):
        self.manifest_path = manifest_path
        self.workflows_yaml_name = workflows_yaml_name
        self.params = params

        parser = DbtManifestParser(manifest_file_path=manifest_path)
        self.dag = parser.parse_manifest()
        self.yaml_builder = YamlLib(params)
        self.flow_builder = FlowBuilder(self.dag)

    def create(self):
        task_list = self.flow_builder.create_task_list()
        task_structure = self.flow_builder.create_task_structure()

        tasks_yaml = self.yaml_builder.create_tasks_yaml_list(task_structure)

        result = self.yaml_builder.create_workflow(task_list, tasks_yaml)

        return result

    def convert(self) -> None:
        yaml_obj = self.create()
        with open(self.workflows_yaml_name, "w") as file:
            yaml.dump(yaml_obj, file)
