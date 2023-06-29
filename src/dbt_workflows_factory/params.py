from __future__ import annotations

from typing import NamedTuple


class Params(NamedTuple):
    image_uri: str
    region: str
    full_command: str
    remote_path: str
    key_volume_mount_path: str
    key_volume_path: str
    key_path: str
