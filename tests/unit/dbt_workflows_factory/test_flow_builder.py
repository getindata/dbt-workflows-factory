from dbt_graph_builder.builder import create_tasks_graph, load_dbt_manifest

from dbt_workflows_factory.flow_builder import FlowBuilder
from dbt_workflows_factory.task import SingleTask


def test_flow_builder():
    manifest_graph = create_tasks_graph(load_dbt_manifest("tests/unit/dbt_workflows_factory/test_data/manifest.json"))
    flow_builder = FlowBuilder(manifest_graph)
    assert flow_builder.create_task_list() == [
        "model.pipeline_example.orders",
        "model.pipeline_example.supplier_parts",
        "model.pipeline_example.all_europe_region_countries",
        "model.pipeline_example.report",
    ]
    assert flow_builder.create_task_structure() == [
        SingleTask(job_id="model.pipeline_example.orders", task_command="orders", task_alias="orders"),
        [
            [
                SingleTask(
                    job_id="model.pipeline_example.supplier_parts",
                    task_command="supplier_parts",
                    task_alias="supplier_parts",
                )
            ],
            [
                SingleTask(
                    job_id="model.pipeline_example.all_europe_region_countries",
                    task_command="all_europe_region_countries",
                    task_alias="all_europe_region_countries",
                )
            ],
        ],
        SingleTask(job_id="model.pipeline_example.report", task_command="report", task_alias="report"),
    ]
