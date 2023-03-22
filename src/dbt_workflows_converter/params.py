class Params:
    def __init__(
        self,
        image_uri: str,
        region: str,
        full_command,
        remote_path,
        key_volume_mount_path,
        key_volume_path,
        key_path,
    ):
        self.image_uri = image_uri
        self.region = region
        self.full_command = full_command
        self.remote_path = remote_path
        self.key_volume_mount_path = key_volume_mount_path
        self.key_volume_path = key_volume_path
        self.key_path = key_path
