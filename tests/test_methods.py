from __future__ import annotations

from dbt_workflows_converter.dag_factory.dag_factory import DbtManifestParser
from dbt_workflows_converter.flow_builder import FlowBuilder


def test_parser_test():
    parser = DbtManifestParser(manifest_file_path="manifest.json")
    G = parser.parse_manifest()
    assert (G.number_of_nodes, 4)
    assert (G.number_of_edges, 4)


def test_flowbuilder_test():
    parser = DbtManifestParser(manifest_file_path="manifest.json")
    g = parser.parse_manifest()
    f = FlowBuilder(g)
    # f.create_task_list()
    print(f.create_task_list())
