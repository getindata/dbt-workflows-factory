import networkx as nx
from dbt_graph_builder.builder import create_tasks_graph, load_dbt_manifest


class DbtManifestParser:
    def __init__(self):
        self.manifest_file_path = None

    def parse_manifest(self, manifest_file_path: str) -> nx.DiGraph:
        return graph
