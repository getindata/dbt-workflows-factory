import json

from click.testing import CliRunner

from dbt_workflows_factory.cli import convert, create_request


def test_convert(monkeypatch):
    def get_yaml(self, *args, **kwargs):
        return {
            "params": str(self._params),
            "manifest_path": self._manifest_path,
        }

    monkeypatch.setattr("dbt_workflows_factory.cli.DbtWorkflowsConverter.get_yaml", get_yaml)
    runner = CliRunner()
    result = runner.invoke(
        convert,
        [
            "--image-uri",
            "image_uri",
            "--region",
            "region",
            "--full-command",
            "full_command",
            "--remote-path",
            "remote_path",
            "--key-volume-mount-path",
            "key_volume_mount_path",
            "--key-volume-path",
            "key_volume_path",
            "--key-path",
            "key_path",
            "tests/unit/dbt_workflows_factory/test_data/manifest.json",
        ],
    )
    params_result = json.dumps(
        {
            "params": (
                "Params("
                "image_uri='image_uri', "
                "region='region', "
                "full_command='full_command', "
                "remote_path='remote_path', "
                "key_volume_mount_path='key_volume_mount_path', "
                "key_volume_path='key_volume_path', "
                "key_path='key_path')"
            ),
            "manifest_path": "tests/unit/dbt_workflows_factory/test_data/manifest.json",
        }
    )
    assert result.output == f"{params_result}\n"
    assert result.exit_code == 0


def test_create_request():
    runner = CliRunner()
    result = runner.invoke(
        create_request,
        [
            "--name",
            "name",
            "--description",
            "description",
            "--labels",
            "label1",
            "value1",
            "--labels",
            "label2",
            "value2",
            "--service-account",
            "service_account",
            "--source-contents",
            "source_contents",
        ],
    )
    assert result.output == (
        json.dumps(
            {
                "name": "name",
                "description": "description",
                "labels": {"label1": "value1", "label2": "value2"},
                "sourceContents": "source_contents",
                "serviceAccount": "service_account",
            }
        )
        + "\n"
    )
