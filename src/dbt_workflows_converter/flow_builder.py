import networkx as nx
from dbt_workflows_converter.task_builder import Task


class FlowBuilder:
    def __init__(self, task_graph: nx.DiGraph):
        self.graph = task_graph
        self.branch_counter = 0

    def create_task_list(self):
        tasks_names = [node for node in self.graph.nodes()]
        return tasks_names

    def create_task_structure(self):
        source_nodes = [node for node in self.graph.nodes() if self.graph.in_degree(node) == 0]
        sink_nodes = [node for node in self.graph.nodes() if self.graph.out_degree(node) == 0]

        if len(sink_nodes) != 1:
            raise ValueError("Manifest DAG must have exactly one sink node")
        sink_node = sink_nodes[0]

        def find_paths(node):
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
                next_paths = find_paths(next_node)
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

        path_list = [source_node for source_node in source_nodes]
        for source_node in source_nodes:
            paths = find_paths(source_node)
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
        for i in range(1, len(path_list) - 1):
            element = path_list[i]
            if isinstance(element, list):
                for e in element:
                    if e[0] == path_list[i - 1]:
                        del e[0]
                    if e[-1] == path_list[i + 1]:
                        del e[-1]

        return path_list
