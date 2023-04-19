from __future__ import annotations

from src.dbt_workflows_factory.dag_factory.dag_factory import (
    DbtManifestParser,
)
from src.dbt_workflows_factory.flow_builder import FlowBuilder


def test_parser_test():
    parser = DbtManifestParser(manifest_file_path="manifest.json")
    G = parser.parse_manifest()
    assert (
        G.nodes,
        [
            "model.pipeline_example.orders",
            "model.pipeline_example.supplier_parts",
            "model.pipeline_example.all_europe_region_countries",
            "model.pipeline_example.report",
        ],
    )
    assert (
        G.edges,
        [
            (
                "model.pipeline_example.orders",
                "model.pipeline_example.supplier_parts",
            ),
            (
                "model.pipeline_example.orders",
                "model.pipeline_example.all_europe_region_countries",
            ),
            (
                "model.pipeline_example.supplier_parts",
                "model.pipeline_example.report",
            ),
            (
                "model.pipeline_example.all_europe_region_countries",
                "model.pipeline_example.report",
            ),
        ],
    )
    assert (G.number_of_nodes, 4)
    assert (G.number_of_edges, 4)


def test_flowbuilder_test():
    parser = DbtManifestParser(manifest_file_path="manifest.json")
    g = parser.parse_manifest()
    f = FlowBuilder(g)
    assert (
        f.create_task_list(),
        [
            "model.pipeline_example.orders",
            "model.pipeline_example.supplier_parts",
            "model.pipeline_example.all_europe_region_countries",
            "model.pipeline_example.report",
        ],
    )
