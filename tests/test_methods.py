
from __future__ import annotations

from src.DbtWorkflowsConverter.dbt_workflows_converter import DbtWorkflowsConverter


def test_DbtWorkflowsConverter():
    """
    This defines the expected usage, which can then be used in various test cases.
    Pytest will not execute this code directly, since the function does not contain the suffex "test"
    """
    conv = DbtWorkflowsConverter("manifest.json", "")
    result = conv.create_tasks()

