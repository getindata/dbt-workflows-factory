from __future__ import annotations

from dbt_graph_builder.builder import (
    DbtManifestGraph,
    GraphConfiguration,
    create_tasks_graph,
    load_dbt_manifest,
)


class DbtManifestParser:
    @staticmethod
    def parse_manifest(
        manifest_file_path: str, graph_config: GraphConfiguration = GraphConfiguration()
    ) -> DbtManifestGraph:
        return create_tasks_graph(load_dbt_manifest(manifest_file_path), graph_config)
