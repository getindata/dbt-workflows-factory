import yaml
from .dag_factory.dag_factory import DbtManifestParser
from .flow_builder import FlowBuilder
from .params import Params
from .yaml_builder import YamlLib


class DbtWorkflowsConverter:
    def __init__(
        self,
        params: Params,
        manifest_path: str = "manifest.json",
        workflows_yaml_name: str = "workflow.yaml",
    ):
        self.flow_builder = None
        self.yaml_builder = None
        self.dag = None
        self.manifest_path = manifest_path
        self.workflows_yaml_name = workflows_yaml_name
        self.params = params

        self.parser = DbtManifestParser()

    def create(self):
        self.dag = self.parser.parse_manifest(manifest_file_path=self.manifest_path)
        self.yaml_builder = YamlLib(self.params)
        self.flow_builder = FlowBuilder(self.dag)

        task_list = self.flow_builder.create_task_list(self.dag)
        task_structure = self.flow_builder.create_task_structure()

        tasks_yaml = self.yaml_builder.create_tasks_yaml_list(task_structure)

        result = self.yaml_builder.create_workflow(task_list, tasks_yaml)

        return result

    def convert(self) -> None:
        yaml_obj = self.create()
        with open(self.workflows_yaml_name, "w") as file:
            yaml.dump(yaml_obj, file)
