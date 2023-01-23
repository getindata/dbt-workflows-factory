from src.python_package.utils.config_utils import read_config

class DbtGraphFactory:
    # For now dict str, str -> [task_id]-[task_id], will return yaml later
    # to task.
    graph: dict[str, str]
    manifest_path: str

    def __init__(
        self,
        manifest_path: str,
    ):
        self.manifest_path = manifest_path
        self.dbt_config = self._read_config(manifest_path)

    def create(self):
        pass

    def create_start_task(self):
        start = self._create_starting_task()
        self.graph[start] = start
        pass

    def _create_starting_task(self) -> str:
        if self.dbt_config.get("seed_task", True):
            ### add builder to create seed task
            pass
        else:
            ### return dummy starting task
            pass

    def _read_config(self, dag_path):
        config = read_config(dag_path)
        return config
        pass