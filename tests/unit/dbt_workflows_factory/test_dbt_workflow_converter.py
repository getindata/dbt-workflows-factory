from dbt_workflows_factory.dbt_workflows_converter import DbtWorkflowsConverter
from dbt_workflows_factory.params import Params


def test_converter():
    converter = DbtWorkflowsConverter(
        params=Params(
            image_uri="image_uri",
            region="region",
            gcs_key_volume_remote_path="remote_path",
            gcs_key_volume_mount_path="key_volume_mount_path",
            gcs_key_volume_container_mount_path="key_volume_path",
            container_gcp_key_path="key_path",
            container_gcp_project_id="project_id",
        ),
        manifest_path="tests/unit/dbt_workflows_factory/test_data/manifest.json",
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
                            {"imageUri": "image_uri"},
                        ]
                    }
                },
                {
                    "orders": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "orders",
                            "jobId": "model_pipeline_example_orders",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_orders_RESULT",
                    }
                },
                {
                    "orders": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "orders",
                            "jobId": "model_pipeline_example_orders",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_pipeline_example_orders_RESULT",
                    }
                },
                {
                    "parallel-2": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain-5": {
                                        "steps": [
                                            {
                                                "supplier_parts": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "supplier_parts",
                                                        "jobId": "model_pipeline_example_supplier_parts",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_supplier_parts_RESULT",
                                                }
                                            },
                                            {
                                                "supplier_parts": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "supplier_parts",
                                                        "jobId": "model_pipeline_example_supplier_parts",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_pipeline_example_supplier_parts_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain-8": {
                                        "steps": [
                                            {
                                                "all_europe_region_countries": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "all_europe_region_countries",
                                                        "jobId": "model_pipeline_example_all_europe_region_countries",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_all_europe_region_countries_RESULT",
                                                }
                                            },
                                            {
                                                "all_europe_region_countries": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "all_europe_region_countries",
                                                        "jobId": "model_pipeline_example_all_europe_region_countries",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_pipeline_example_all_europe_region_countries_RESULT",
                                                }
                                            },
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
                            "select": "report",
                            "jobId": "model_pipeline_example_report",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_report_RESULT",
                    }
                },
                {
                    "report": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "report",
                            "jobId": "model_pipeline_example_report",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_pipeline_example_report_RESULT",
                    }
                },
            ]
        },
        "subworkflowBatchJob": {
            "params": ["batchApiUrl", "command", "jobId", "imageUri", "select"],
            "steps": [
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
                                                    "entrypoint": "/bin/bash",
                                                    "commands": [
                                                        "-c",
                                                        "dbt --no-write-json ${command} --target env_execution --project-dir /dbt --profiles-dir /root/.dbt --select ${select}",
                                                    ],
                                                    "volumes": ["key_volume_path"],
                                                },
                                                "environment": {
                                                    "variables": {
                                                        "GCP_KEY_PATH": "key_path",
                                                        "GCP_PROJECT": "project_id",
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


def test_converter_2():
    converter = DbtWorkflowsConverter(
        params=Params(
            image_uri="image_uri",
            region="region",
            gcs_key_volume_remote_path="remote_path",
            gcs_key_volume_mount_path="key_volume_mount_path",
            gcs_key_volume_container_mount_path="key_volume_path",
            container_gcp_key_path="key_path",
            container_gcp_project_id="project_id",
        ),
        manifest_path="tests/unit/dbt_workflows_factory/test_data/manifest_2.json",
    )
    assert converter.get_yaml(show_ephemeral_models=True) == {
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
                            {"imageUri": "image_uri"},
                        ]
                    }
                },
                {
                    "parallel-7": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain-18": {
                                        "steps": [
                                            {
                                                "model1": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model1",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model1",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model1_RESULT",
                                                }
                                            },
                                            {
                                                "model1": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model1",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model1",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model1_RESULT",
                                                }
                                            },
                                            {
                                                "model2": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model2",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model2",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model2_RESULT",
                                                }
                                            },
                                            {
                                                "model2": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model2",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model2",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model2_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain-25": {
                                        "steps": [
                                            {
                                                "model5": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model5",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model5",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model5_RESULT",
                                                }
                                            },
                                            {
                                                "model5": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model5",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model5",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model5_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain-28": {
                                        "steps": [
                                            {
                                                "model6": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model6",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model6",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model6_RESULT",
                                                }
                                            },
                                            {
                                                "model6": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model6",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model6",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model6_RESULT",
                                                }
                                            },
                                            {
                                                "parallel-6": {
                                                    "parallel": {
                                                        "branches": [
                                                            {
                                                                "chain-31": {
                                                                    "steps": [
                                                                        {
                                                                            "model7": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model_dbt_test_model7",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model7",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model7_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model7": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model_dbt_test_model7",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model7",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "test",
                                                                                },
                                                                                "result": "model_dbt_test_model7_RESULT",
                                                                            }
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "chain-34": {
                                                                    "steps": [
                                                                        {
                                                                            "model8": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model_dbt_test_model8",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model8",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model8_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model8": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model_dbt_test_model8",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model8",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "test",
                                                                                },
                                                                                "result": "model_dbt_test_model8_RESULT",
                                                                            }
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                        ]
                                                    }
                                                }
                                            },
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                },
                {
                    "parallel-9": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain-39": {
                                        "steps": [
                                            {
                                                "model3": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model3",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model3",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model3_RESULT",
                                                }
                                            },
                                            {
                                                "model3": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model3",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model3",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model3_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain-42": {
                                        "steps": [
                                            {
                                                "model9": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model9",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model9",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model9_RESULT",
                                                }
                                            },
                                            {
                                                "model9": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model9",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model9",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model9_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                },
                {
                    "model10": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model_dbt_test_model10",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "model10",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_dbt_test_model10_RESULT",
                    }
                },
                {
                    "model10": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model_dbt_test_model10",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "model10",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_dbt_test_model10_RESULT",
                    }
                },
                {
                    "parallel-12": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain-49": {
                                        "steps": [
                                            {
                                                "model4": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model4",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model4",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model4_RESULT",
                                                }
                                            },
                                            {
                                                "model4": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model_dbt_test_model4",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model4",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model4_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain-50": {
                                        "steps": [
                                            {
                                                "call": "sys.log",
                                                "args": {
                                                    "text": "Skipping ephemeral node: model11",
                                                    "severity": "INFO",
                                                },
                                            }
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                },
                {"call": "sys.log", "args": {"text": "Skipping ephemeral node: model12", "severity": "INFO"}},
            ]
        },
        "subworkflowBatchJob": {
            "params": ["batchApiUrl", "command", "jobId", "imageUri", "select"],
            "steps": [
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
                                                    "entrypoint": "/bin/bash",
                                                    "commands": [
                                                        "-c",
                                                        "dbt --no-write-json ${command} --target env_execution --project-dir /dbt --profiles-dir /root/.dbt --select ${select}",
                                                    ],
                                                    "volumes": ["key_volume_path"],
                                                },
                                                "environment": {
                                                    "variables": {
                                                        "GCP_KEY_PATH": "key_path",
                                                        "GCP_PROJECT": "project_id",
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
