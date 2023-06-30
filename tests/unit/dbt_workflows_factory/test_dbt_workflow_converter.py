from dbt_workflows_factory.dbt_workflows_converter import DbtWorkflowsConverter
from dbt_workflows_factory.params import Params
from pprint import pformat
import json

def test_converter():
    converter = DbtWorkflowsConverter(
        params=Params(
            image_uri="image_uri",
            region="region",
            full_command="full_command",
            remote_path="remote_path",
            key_volume_mount_path="key_volume_mount_path",
            key_volume_path="key_volume_path",
            key_path="key_path",
        ),
        manifest_path="tests/unit/dbt_workflows_factory/test_data/manifest.json",
        workflows_path="tests/unit/dbt_workflows_factory/test_data/workflows.json",
    )
    print()
    print(json.dumps(converter.get_yaml(), indent=4))
