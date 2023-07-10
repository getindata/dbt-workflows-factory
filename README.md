# dbt-workflows-factory

[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11-blue)](https://github.com/getindata/dbt-workflows-factory)
[![PyPI Version](https://badge.fury.io/py/dbt-workflows-factory.svg)](https://pypi.org/project/dbt-workflows-factory/)
[![Downloads](https://pepy.tech/badge/dbt-workflows-factory)](https://pepy.tech/project/dbt-workflows-factory)

Creates dbt based GCP workflows.

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install [dbt-workflows-factory](https://pypi.org/project/dbt-workflows-factory/) for [dp (data-pipelines-cli)]:

```bash
pip install dbt-workflows-factory
```


### How to run

To call from cli, you can

```bash
python -m dbt_workflows_factory.cli convert \
    --image-uri my-image-uri \
    --gcs-key-volume-remote-path google-cloud-storage/path/ \
    --gcs-key-volume-mount-path /etc/gcs-key/ \
    --gcs-key-volume-container-mount-path /etc/gcs-key/:/etc/gcs-key/:ro \
    --container-gcp-key-path /etc/gcs-key/path.json \
    --container-gcp-project-id some-project-id \
    --pretty \
    tests/unit/dbt_workflows_factory/test_data/manifest.json > workflow_source.json
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
