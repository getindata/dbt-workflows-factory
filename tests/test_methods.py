from __future__ import annotations

from src.DbtWorkflowsConverter.dbt_workflows_converter import DbtWorkflowsConverter
from src.DbtWorkflowsConverter.dbt_workflows_converter import DbtWorkflowsConverterParams


def flow_test():
    params = DbtWorkflowsConverterParams(dbt_manifest_path="manifest.json", env="env")
    converter = DbtWorkflowsConverter(params)

    result = converter.create_tasks()
    assert (result.len != 0)
    # Todo add comparing after conterter.write_tasks