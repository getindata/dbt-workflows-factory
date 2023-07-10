from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone

import click

from .dbt_workflows_converter import DbtWorkflowsConverter
from .params import Params


@click.group()
def cli() -> None:
    """CLI entrypoint."""


@cli.command()
@click.argument("manifest_file", type=click.Path(exists=True))
@click.option("--image-uri", type=str, help="Docker image URI", required=True)
@click.option(
    "--location",
    type=str,
    help="GCP region",
    required=False,
    default='${sys.get_env("GOOGLE_CLOUD_LOCATION")}',
    show_default=True,
)
@click.option(
    "--project-id",
    type=str,
    help="GCP project ID",
    required=False,
    default='${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}',
    show_default=True,
)
@click.option("--gcs-key-volume-remote-path", type=str, help="GCS Remote path for private key", required=True)
@click.option(
    "--gcs-key-volume-mount-path",
    type=str,
    help="Volume mount path for private key",
    required=True,
)
@click.option(
    "--gcs-key-volume-container-mount-path",
    type=str,
    help="Volume mount path for private key in container",
    required=True,
)
@click.option(
    "--container-gcp-key-path",
    type=str,
    help="Path for the private key file in the container",
    required=True,
)
@click.option(
    "--container-gcp-project-id",
    type=str,
    help="GCP project ID in the container",
    required=True,
)
@click.option(
    "--job-id-suffix",
    type=str,
    help="Job ID suffix",
    required=False,
    default=f'"{int(datetime.now(tz=timezone(offset=timedelta(hours=0))).timestamp())}"',
    show_default=True,
)
@click.option(
    "--pretty",
    type=bool,
    help="Pretty print output",
    required=False,
    default=False,
    is_flag=True,
)
def convert(
    manifest_file: str,
    image_uri: str,
    location: str,
    project_id: str,
    gcs_key_volume_remote_path: str,
    gcs_key_volume_mount_path: str,
    gcs_key_volume_container_mount_path: str,
    container_gcp_key_path: str,
    container_gcp_project_id: str,
    job_id_suffix: str,
    pretty: bool,
) -> None:
    """Convert dbt manifest.json to YAML for GCP Workflows."""  # noqa: DCO020
    params = Params(
        image_uri=image_uri,
        location=location,
        project_id=project_id,
        gcs_key_volume_remote_path=gcs_key_volume_remote_path,
        gcs_key_volume_mount_path=gcs_key_volume_mount_path,
        gcs_key_volume_container_mount_path=gcs_key_volume_container_mount_path,
        container_gcp_key_path=container_gcp_key_path,
        container_gcp_project_id=container_gcp_project_id,
        job_id_suffix=job_id_suffix,
    )
    converter = DbtWorkflowsConverter(manifest_path=manifest_file, params=params)
    click.echo(json.dumps(converter.get_yaml(), indent=2 if pretty else None))


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
