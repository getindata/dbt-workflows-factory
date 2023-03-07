class WorkflowParameters:
    def __init__(
        self,
        region: str = "europe-west6",
        command: str = ('${"dbt --no-write-json run --target env_execution '
            '--project-dir /dbt --profiles-dir /root/.dbt --select " + '
            "command }"),
        key_volume: str = "/mnt/disks/var/:/mnt/disks/var/:rw",
        key_remote_path: str = None
    ):
        self.region = region
        self.command = command
        self.key_volume = key_volume
        self.key_remote_path = key_remote_path

    def get_region(self):
        return self.region

    def get_command(self):
        return self.command

    def get_key_volume_mount_path(self):
        return self.get_key_volume_mount_path

    def get_key_volume_path(self):
        if self.key_volume is None:
            return None
        return self.key_volume + "/:" + self.key_volume + "/:rw"

    def get_key_remote_path(self):
        return self.key_remote_path