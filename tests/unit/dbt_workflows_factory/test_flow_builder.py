from dbt_graph_builder.builder import create_tasks_graph,load_dbt_manifest
from dbt_workflows_factory.flow_builder import FlowBuilder
from pprint import pformat


def test_flow_builder():
    manifest_graph = create_tasks_graph(load_dbt_manifest("tests/unit/dbt_workflows_factory/test_data/manifest.json"))
    flow_builder = FlowBuilder(manifest_graph)
    print(flow_builder.create_task_list())
    print(pformat(flow_builder.create_task_structure()))


def test_flow_builder_task_list():
    graph = create_tasks_graph(load_dbt_manifest("tests/unit/dbt_workflows_factory/test_data/manifest.json"))
    flow_builder = FlowBuilder(graph)
    assert (
        flow_builder.create_task_list(),
        [
            "model.pipeline_example.orders",
            "model.pipeline_example.supplier_parts",
            "model.pipeline_example.all_europe_region_countries",
            "model.pipeline_example.report",
        ],
    )
