from __future__ import annotations

from typing import NamedTuple


class Params(NamedTuple):
    """Parameters for workflows."""

    image_uri: str
    location: str
    project_id: str
    gcs_key_volume_remote_path: str
    gcs_key_volume_mount_path: str
    gcs_key_volume_container_mount_path: str
    container_gcp_key_path: str
    container_gcp_project_id: str
    job_id_suffix: str
