from __future__ import annotations

import json
import sys

import click

from .dbt_workflows_converter import DbtWorkflowsConverter
from .params import Params


@click.group()
def cli() -> None:
    """CLI entrypoint."""


@cli.command()
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
    manifest_file: str,
    image_uri: str,
    region: str,
    full_command: str,
    remote_path: str,
    key_volume_mount_path: str,
    key_volume_path: str,
    key_path: str,
) -> None:
    """Convert dbt manifest.json to YAML for GCP Workflows."""  # noqa: DCO020
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


@cli.command()
@click.option("--name", type=str, help="Workflow name", required=True)
@click.option("--description", type=str, help="Workflow description", required=False)
@click.option("--labels", "-l", type=(str, str), help="Workflow labels", required=False, multiple=True)
@click.option("--service-account", type=str, help="Service account", required=True)
@click.option("--source-contents", type=str, help="Source contents", required=False)
@click.option("--source-path", type=click.Path(exists=True), help="Source path", required=False)
def create_request(
    name: str,
    description: str,
    labels: list[tuple[str, str]],
    service_account: str,
    source_contents: str | None,
    source_path: str | None,
) -> None:
    """Create a request for a new workflow."""  # noqa: DCO020
    if source_contents is None and source_path is None:
        source_contents = sys.stdin.read()
    elif source_path is not None:
        with open(source_path) as file:
            source_contents = file.read()
    click.echo(
        json.dumps(
            {
                "name": name,
                "description": description,
                "labels": dict(labels),
                "sourceContents": source_contents,
                "serviceAccount": service_account,
            }
        )
    )


if __name__ == "__main__":
    cli()
