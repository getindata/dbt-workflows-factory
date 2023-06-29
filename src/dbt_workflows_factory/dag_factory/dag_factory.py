from dbt_graph_builder.builder import create_tasks_graph, load_dbt_manifest, GraphConfiguration, DbtManifestGraph


class DbtManifestParser:
    @staticmethod
    def parse_manifest(manifest_file_path: str, graph_config: GraphConfiguration = GraphConfiguration()) -> DbtManifestGraph:
        dbt_manifest = load_dbt_manifest(manifest_file_path)
        dag = create_tasks_graph(dbt_manifest, graph_config)
        return dag
