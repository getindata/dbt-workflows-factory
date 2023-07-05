# dbt-workflows-converter

[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11-blue)](https://github.com/getindata/dbt-workflows-converter)
[![PyPI Version](https://badge.fury.io/py/dbt-workflows-converter.svg)](https://pypi.org/project/dbt-workflows-converter/)
[![Downloads](https://pepy.tech/badge/dbt-workflows-converter)](https://pepy.tech/project/dbt-workflows-converter)
[![Maintainability](https://api.codeclimate.com/v1/badges/e44ed9383a42b59984f6/maintainability)](https://codeclimate.com/github/getindata/dbt-workflows-converter/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/e44ed9383a42b59984f6/test_coverage)](https://codeclimate.com/github/getindata/dbt-workflows-converter/test_coverage)
[![Documentation Status](https://readthedocs.org/projects/dbt-workflows-converter/badge/?version=latest)](https://dbt-workflows-converter.readthedocs.io/en/latest/?badge=latest)

Creates dbt based GCP workflows.

[//]: # (## Documentation)

[//]: # ()
[//]: # (Read the full documentation at [https://dbt-workflows-converter.readthedocs.io/]&#40;https://dbt-workflows-converter.readthedocs.io/en/latest/index.html&#41;)

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install [dbt-workflows-converter](https://pypi.org/project/dbt-workflows-converter/) for [dp (data-pipelines-cli)]:

```bash
pip install dbt-workflows-converter
```

## Usage

In order to run the dbt-workflows-converter, you will need:
- `manifest.json` file with dbt manifest
- `params` for the special parameters.

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

While running, you can specify the location of manifest file and the yaml file, but they are set by default to: "manifest.json" and "workflow.yaml".

```python
import DbtWorkflowsConverter

converter = DbtWorkflowsConverter(params)
converter.convert() # writes to file workflow.yaml

```

To call from cli, you can

```
python src/dbt_workflows_converter/compile.py tests/manifest.json --image-uri xxx --region xxx --full-command xxx --remote-path xxx --key-volume-mount-path xxx --key-volume-path xxx --key-path xxx

```

## Project Organization

- .devcontainer - This directory contains required files for creating a [Codespace](https://github.com/features/codespaces).
- .github
  - workflows - Contains GitHub Actions used for building, testing and publishing.
    - publish-test.yml - Publish wheels to [https://test.pypi.org/](https://test.pypi.org/)
    - publish.yml - Publish wheels to [https://pypi.org/](https://pypi.org/)
    - pull-request.yml - Build and Test pull requests before commiting to main.
    - template-sync.yml - Update GitHub Repo with enhancments to base template
- docs - collect documents (default format .md)
- src - place new source code here
  - python_package - sample package (this can be deleted when creating a new repository)
- tests - contains Python based test cases to validation src code
- .pre-commit-config.yaml - Contains various pre-check fixes for Python
- pyproject.toml - Python Project Declaration
- ws.code-workspace - Recommended configurations for [Visual Studio Code](https://code.visualstudio.com/)

## Publish to PyPi from GitHub
In order to publish to PyPi, a repostirory secret must be created, "PYPI_PASSWORD". In order to publish to the Test PyPi, a second secret must be added, "TEST_PYPI_PASSWORD".
