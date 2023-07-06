# dbt-workflows-factory

[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11-blue)](https://github.com/getindata/dbt-workflows-factory)
[![PyPI Version](https://badge.fury.io/py/dbt-workflows-factory.svg)](https://pypi.org/project/dbt-workflows-factory/)
[![Downloads](https://pepy.tech/badge/dbt-workflows-factory)](https://pepy.tech/project/dbt-workflows-factory)

Creates dbt based GCP workflows.

[//]: # (## Documentation)

[//]: # ()
[//]: # (Read the full documentation at [https://dbt-workflows-factory.readthedocs.io/]&#40;https://dbt-workflows-factory.readthedocs.io/en/latest/index.html&#41;)

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install [dbt-workflows-factory](https://pypi.org/project/dbt-workflows-factory/) for [dp (data-pipelines-cli)]:

```bash
pip install dbt-workflows-factory
```
### Params

Parameters specified for the converters are:
1. `image_uri`: url address for the image
2. `region`: the location where tge workflow executes on GCP (example: `us-central1` or `europe-west1`)
3. `full_command`: full command executed on image (example: `"dbt --no-write-json run --target env_execution --project-dir /dbt --profiles-dir /root/.dbt --select "`)
4. `remote_path`: gcs mount path (example: ` "/mnt/disks/var"`)
5. `key_volume_mount_path`: path for mounting the volume containg key (ex. `/mnt/disks/var/keyfile_name.json`)
6. `key_volume_path`: path for mounting (ex. `["/mnt/disks/var/:/mnt/disks/var/:rw"]`)
7. `key_path`:  is a remote path for bucket containing key to be mounted

### How to run

To call from cli, you can

```
python -m dbt_workflows_factory.cli path/to/manifest.json --image-uri xxx --region xxx --full-command xxx --remote-path xxx --key-volume-mount-path xxx --key-volume-path xxx --key-path xxx

```

## Project Organization

- .devcontainer - This directory contains required files for creating a [Codespace](https://github.com/features/codespaces).
- .github
  - workflows - Contains GitHub Actions used for building, testing and publishing.
    - publish.yml - Publish wheels to [https://pypi.org/](https://pypi.org/)
    - pull-request.yml - Build and Test pull requests before commiting to main.
    - template-sync.yml - Update GitHub Repo with enhancments to base template
- docs - collect documents (default format .md)
- src - place new source code here
  - python_package - sample package (this can be deleted when creating a new repository)
- tests - contains Python based test cases to validation src code
- .pre-commit-config.yaml - Contains various pre-check fixes for Python
- pyproject.toml - Python Project Declaration

## Publish to PyPi from GitHub
In order to publish to PyPi, a repostirory secret must be created, "PYPI_PASSWORD". In order to publish to the Test PyPi, a second secret must be added, "TEST_PYPI_PASSWORD".
