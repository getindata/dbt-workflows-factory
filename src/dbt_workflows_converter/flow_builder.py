import networkx as nx
from src.dbt_workflows_converter.task_builder import Task


class FlowBuilder:
    def __init__(self, task_graph: nx.DiGraph):
        self.graph = task_graph

    def create_task_list(self, graph):
        return graph.nodes()

    def find_paths(self, node, sink_node):
        if node == sink_node:
            return [
                [
                    Task(
                        self.graph.nodes[node],
                        self.graph.nodes[node].get("task_command"),
                        self.graph.nodes[node].get("job_id"),
                    )
                ]
            ]
        paths = []
        next_nodes = self.graph.successors(node)
        for next_node in next_nodes:
            next_paths = self.find_paths(next_node, sink_node)
            for path in next_paths:
                paths.append(
                    [
                        Task(
                            self.graph.nodes[node],
                            self.graph.nodes[node].get("task_command"),
                            self.graph.nodes[node].get("job_id"),
                        )
                    ]
                    + path
                )
        return paths

    def clear_structure(self, structure):
        for i in range(1, len(structure) - 1):
            branch = structure[i]
            if isinstance(branch, list):
                previous_task_id = structure[i - 1].job_id
                next_task_id = structure[i + 1].job_id

                for t in branch:
                    first_id_in_branch = t[0].job_id
                    last_id_in_branch = t[-1].job_id
                    if first_id_in_branch == previous_task_id:
                        del t[0]
                    elif last_id_in_branch == next_task_id:
                        del t[-1]

    def create_task_structure(self):
        source_nodes = [node for node in self.graph.nodes() if self.graph.in_degree(node) == 0]
        sink_nodes = [node for node in self.graph.nodes() if self.graph.out_degree(node) == 0]

        if len(sink_nodes) != 1:
            raise ValueError("Manifest DAG must have exactly one sink node")
        sink_node = sink_nodes[0]

        path_list = [source_node for source_node in source_nodes]
        for source_node in source_nodes:
            paths = self.find_paths(source_node, sink_node)
            if len(paths) > 1:
                path_list.append(paths)
            else:
                path_list.append(paths[0])
        path_list.append(
            Task(
                self.graph.nodes[sink_node],
                self.graph.nodes[sink_node].get("task_command"),
                self.graph.nodes[sink_node].get("job_id"),
            )
        )

        self.clear_structure(path_list)

        return path_list
