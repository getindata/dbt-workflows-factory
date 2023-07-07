#!/bin/bash

python -m dbt_workflows_factory.cli convert \
    --image-uri my-image-uri \
    --region europe-west6 \
    --gcs-key-volume-remote-path google-cloud-storage/path/ \
    --gcs-key-volume-mount-path /etc/gcs-key/ \
    --gcs-key-volume-container-mount-path /etc/gcs-key/:/etc/gcs-key/:ro \
    --container-gcp-key-path /etc/gcs-key/path.json \
    --container-gcp-project-id gid-dataops-labs \
    --pretty \
    tests/unit/dbt_workflows_factory/test_data/manifest.json > workflow_source.json

python -m dbt_workflows_factory.cli create-request \
    --name test-workflow-factory-1 \
    --service-account my-service-account \
    --source-path workflow_source.json > workflow_request.json

# curl --request POST \
#   'https://workflows.googleapis.com/v1/projects/project_id/locations/location_id/workflows?workflowId=my-workflow&key=[YOUR_API_KEY]' \
#   --header 'Authorization: Bearer [YOUR_ACCESS_TOKEN]' \
#   --header 'Accept: application/json' \
#   --header 'Content-Type: application/json' \
#   --data @workflow_request.json \
#   --compressed
