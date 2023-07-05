from __future__ import annotations

import json

import click

from .dbt_workflows_converter import DbtWorkflowsConverter
from .params import Params


@click.command()
@click.argument("manifest_file", type=click.Path(exists=True))
@click.option("--image-uri", type=str, help="Docker image URI", required=True)
@click.option("--region", type=str, help="GCP region", required=True)
@click.option(
    "--full-command",
    type=str,
    help="Full command to run in container",
    required=True,
)
@click.option("--remote-path", type=str, help="Path to remote file", required=True)
@click.option(
    "--key-volume-mount-path",
    type=str,
    help="Volume mount path for private key",
    required=True,
)
@click.option(
    "--key-volume-path",
    type=str,
    help="Path for the private key file on the host",
    required=True,
)
@click.option(
    "--key-path",
    type=str,
    help="Path for the private key file in the container",
    required=True,
)
def convert(
    manifest_file: click.Path,
    image_uri: str,
    region: str,
    full_command: str,
    remote_path: str,
    key_volume_mount_path: str,
    key_volume_path: str,
    key_path: str,
) -> None:
    """Convert dbt manifest.json to YAML for GCP Workflows.

    Args:
        manifest_file (click.Path): Path to dbt manifest.json file.
        image_uri (str): Docker image URI.
        region (str): GCP region.
        full_command (str): Full command to run in container.
        remote_path (str): Path to remote file.
        key_volume_mount_path (str): Volume mount path for private key.
        key_volume_path (str): Path for the private key file on the host.
        key_path (str): Path for the private key file in the container.
    """
    params = Params(
        image_uri=image_uri,
        region=region,
        full_command=full_command,
        remote_path=remote_path,
        key_volume_mount_path=key_volume_mount_path,
        key_volume_path=key_volume_path,
        key_path=key_path,
    )
    converter = DbtWorkflowsConverter(manifest_path=manifest_file, params=params)
    click.echo(json.dumps(converter.get_yaml()))


if __name__ == "__main__":
    convert()
