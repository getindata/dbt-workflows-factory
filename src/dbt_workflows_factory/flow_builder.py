from __future__ import annotations

from dbt_graph_builder.builder import DbtManifestGraph
from networkx.classes.reportviews import NodeView

from .task import SingleTask, Task


class FlowBuilder:
    def __init__(self, graph: DbtManifestGraph):
        self._graph = graph

    def create_task_list(self) -> list[str]:
        return [task[0] for task in self._graph.get_graph_nodes()]

    @property
    def graph(self) -> DbtManifestGraph:
        return self._graph

    def create_task_structure(self):
        source_nodes = self._graph.get_graph_sources()
        sink_nodes = self._graph.get_graph_sinks()

        if len(sink_nodes) != 1:
            raise ValueError(f"Manifest DAG must have exactly one sink node: {sink_nodes}")
        sink_node = sink_nodes[0]
        path_list: list[Task | list[Task]] = [
            SingleTask.from_node(source_node, self._graph.graph.nodes[source_node]) for source_node in source_nodes
        ]
        for source_node in source_nodes:
            paths = self._find_paths(source_node, sink_node)
            if len(paths) > 1:
                path_list.append(paths)
            else:
                path_list.append(paths[0])
        path_list.append(SingleTask.from_node(sink_node, self._graph.graph.nodes[sink_node]))
        self._clear_structure(path_list)
        return path_list

    def _clear_structure(self, structure: list[Task | list[Task]]) -> None:
        for i in range(1, len(structure) - 1):
            branch = structure[i]
            if isinstance(branch, list):
                previous_task_id = structure[i - 1]
                next_task_id = structure[i + 1]

                for t in branch:
                    first_id_in_branch = t[0].job_id
                    last_id_in_branch = t[-1].job_id
                    if first_id_in_branch == previous_task_id.job_id:
                        del t[0]
                    if last_id_in_branch == next_task_id.job_id:
                        del t[-1]

    def _find_paths(self, node: str, sink_node: str) -> list[list[Task]]:
        if node == sink_node:
            return [[SingleTask.from_node(node, self._graph.graph.nodes[node])]]
        paths: list[list[Task]] = []
        next_nodes: list[str] = self._graph.graph.successors(node)
        for next_node in next_nodes:
            next_paths = self._find_paths(next_node, sink_node)
            for path in next_paths:
                paths.append([SingleTask.from_node(node, self._graph.graph.nodes[node])] + path)
        return paths
