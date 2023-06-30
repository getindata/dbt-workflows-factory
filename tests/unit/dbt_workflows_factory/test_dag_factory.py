from __future__ import annotations

from dbt_workflows_factory.dag_factory.dag_factory import DbtManifestParser


def test_parser_test():
    parser = DbtManifestParser()
    G = parser.parse_manifest("tests/unit/dbt_workflows_factory/test_data/manifest.json")
    assert (
        G.graph.nodes,
        [
            "model.pipeline_example.orders",
            "model.pipeline_example.supplier_parts",
            "model.pipeline_example.all_europe_region_countries",
            "model.pipeline_example.report",
        ],
    )
    assert (
        G.graph.edges,
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
    assert (G.graph.number_of_nodes, 4)
    assert (G.graph.number_of_edges, 4)
