import json
from pprint import pformat

from dbt_workflows_factory.dbt_workflows_converter import DbtWorkflowsConverter
from dbt_workflows_factory.params import Params


def test_converter():
    converter = DbtWorkflowsConverter(
        params=Params(
            image_uri="image_uri",
            region="region",
            full_command="full_command",
            remote_path="remote_path",
            key_volume_mount_path="key_volume_mount_path",
            key_volume_path="key_volume_path",
            key_path="key_path",
        ),
        manifest_path="tests/unit/dbt_workflows_factory/test_data/manifest.json",
        workflows_path="tests/unit/dbt_workflows_factory/test_data/workflows.json",
    )
    assert converter.get_yaml() == {
        "main": {
            "steps": [
                {
                    "init": {
                        "assign": [
                            {"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'},
                            {"region": "region"},
                            {"batchApi": "batch.googleapis.com/v1"},
                            {
                                "batchApiUrl": '${"https://" + batchApi + "/projects/" + projectId + "/locations/" + region + "/jobs"}'
                            },
                            {"containerEntrypoint": "bash"},
                            {"imageUri": "image_uri"},
                            {
                                "model.pipeline_example.orders": "${model.pipeline_example.orders + string(int(sys.now()))}"
                            },
                            {
                                "model.pipeline_example.supplier_parts": "${model.pipeline_example.supplier_parts + string(int(sys.now()))}"
                            },
                            {
                                "model.pipeline_example.all_europe_region_countries": "${model.pipeline_example.all_europe_region_countries + string(int(sys.now()))}"
                            },
                            {
                                "model.pipeline_example.report": "${model.pipeline_example.report + string(int(sys.now()))}"
                            },
                        ]
                    }
                },
                {
                    "orders": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "batchApiUrl": "${batchApiUrl}",
                            "command": "orders",
                            "jobId": "model.pipeline_example.orders",
                            "imageUri": "${imageUri}",
                        },
                        "result": "${model.pipeline_example.orders}Result",
                    }
                },
                {
                    "parallelSteps": {
                        "parallel": {
                            "branches": [
                                {
                                    "branch1": {
                                        "steps": [
                                            {
                                                "supplier_parts": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "command": "supplier_parts",
                                                        "jobId": "model.pipeline_example.supplier_parts",
                                                        "imageUri": "${imageUri}",
                                                    },
                                                    "result": "${model.pipeline_example.supplier_parts}Result",
                                                }
                                            }
                                        ]
                                    }
                                },
                                {
                                    "branch2": {
                                        "steps": [
                                            {
                                                "all_europe_region_countries": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "command": "all_europe_region_countries",
                                                        "jobId": "model.pipeline_example.all_europe_region_countries",
                                                        "imageUri": "${imageUri}",
                                                    },
                                                    "result": "${model.pipeline_example.all_europe_region_countries}Result",
                                                }
                                            }
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                },
                {
                    "report": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "batchApiUrl": "${batchApiUrl}",
                            "command": "report",
                            "jobId": "model.pipeline_example.report",
                            "imageUri": "${imageUri}",
                        },
                        "result": "${model.pipeline_example.report}Result",
                    }
                },
            ]
        },
        "subworkflowBatchJob": {
            "params": ["batchApiUrl", "command", "jobId", "imageUri"],
            "steps": [
                {"init": {"assign": [{"fullcomand": "full_command"}]}},
                {
                    "createAndRunBatchJob": {
                        "call": "http.post",
                        "args": {
                            "url": "${batchApiUrl}",
                            "query": {"job_id": "${jobId}"},
                            "headers": {"Content-Type": "application/json"},
                            "auth": {"type": "OAuth2"},
                            "body": {
                                "taskGroups": {
                                    "taskSpec": {
                                        "volumes": [
                                            {"gcs": {"remotePath": "remote_path"}, "mountPath": "key_volume_mount_path"}
                                        ],
                                        "runnables": [
                                            {
                                                "container": {
                                                    "imageUri": "imageUri",
                                                    "entrypoint": "bash",
                                                    "commands": ["-c", "full_command"],
                                                    "volumes": ["key_volume_path"],
                                                },
                                                "environment": {
                                                    "variables": {
                                                        "GCP_KEY_PATH": "key_path",
                                                        "GCP_PROJECT": "dataops-test-project",
                                                    }
                                                },
                                            }
                                        ],
                                    }
                                },
                                "logsPolicy": {"destination": "CLOUD_LOGGING"},
                            },
                        },
                        "result": "createAndRunBatchJobResponse",
                    }
                },
                {
                    "getJob": {
                        "call": "http.get",
                        "args": {"url": '${batchApiUrl + "/" + jobId}', "auth": {"type": "OAuth2"}},
                        "result": "getJobResult",
                    }
                },
            ],
        },
    }
