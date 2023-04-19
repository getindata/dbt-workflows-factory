import json
import networkx as nx
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.graph.manifest import Manifest


class DbtManifestParser:
    def __init__(self):
        self.manifest_file_path = None

    def parse_manifest(self, manifest_file_path: str) -> nx.DiGraph:
        self.manifest_file_path = manifest_file_path
        with open(self.manifest_file_path, "r") as manifest_file:
            manifest_json = json.load(manifest_file)

        manifest = Manifest.from_dict(manifest_json)
        graph = nx.DiGraph()

        for node, task_id in zip(manifest.nodes.values(), manifest.nodes):
            if isinstance(node, ModelNode):
                task_alias = node.alias

                graph.add_node(
                    task_id, task_command=node.name, task_alias=task_alias
                )

                upstream_nodes = []
                for upstream_id in node.depends_on.nodes:
                    if graph.has_node(upstream_id):
                        graph.add_edge(upstream_id, task_id)
                        upstream_nodes.append(upstream_id)

        return graph
