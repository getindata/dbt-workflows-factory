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
            job_id_suffix="suffix",
        ),
        manifest_path="tests/unit/dbt_workflows_factory/test_data/manifest.json",
    )
    assert converter.get_yaml() == {
        "chain_14": {
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
                    "model_pipeline_example_orders": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-orders",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "orders",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_orders_RESULT",
                    }
                },
                {
                    "model_pipeline_example_orders": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-orders",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "orders",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_pipeline_example_orders_RESULT",
                    }
                },
                {
                    "parallel_2": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain_5": {
                                        "steps": [
                                            {
                                                "model_pipeline_example_supplier_parts": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-supplier-parts",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "supplier_parts",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_supplier_parts_RESULT",
                                                }
                                            },
                                            {
                                                "model_pipeline_example_supplier_parts": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-supplier-parts",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "supplier_parts",
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
                                    "chain_8": {
                                        "steps": [
                                            {
                                                "model_pipeline_example_all_europe_region_countries": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-all-europe-region-countries",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "all_europe_region_countries",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_all_europe_region_countries_RESULT",
                                                }
                                            },
                                            {
                                                "model_pipeline_example_all_europe_region_countries": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-all-europe-region-countries",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "all_europe_region_countries",
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
                    "model_pipeline_example_report": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-report",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "report",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_report_RESULT",
                    }
                },
                {
                    "model_pipeline_example_report": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-report",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "report",
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
                {"init": {"assign": [{"jobId": '${jobId + "-" + suffix}'}]}},
                {
                    "createAndRunBatchJob": {
                        "call": "http.post",
                        "args": {
                            "url": "${batchApiUrl}",
                            "query": {"jobId": "${jobId}"},
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
                                                        '${"dbt --no-write-json " + command + " --target env_execution --project-dir /dbt --profiles-dir /root/.dbt --select " + select}',
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
            job_id_suffix="suffix",
        ),
        manifest_path="tests/unit/dbt_workflows_factory/test_data/manifest_2.json",
    )
    assert converter.get_yaml(show_ephemeral_models=True) == {
        "chain_54": {
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
                    "parallel_7": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain_18": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model1": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model1",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model1",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model1_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model1": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model1",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model1",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model1_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model2": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model2",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model2",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model2_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model2": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model2",
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
                                    "chain_25": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model5": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model5",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model5",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model5_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model5": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model5",
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
                                    "chain_28": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model6": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model6",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model6",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model6_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model6": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model6",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model6",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model6_RESULT",
                                                }
                                            },
                                            {
                                                "parallel_6": {
                                                    "parallel": {
                                                        "branches": [
                                                            {
                                                                "chain_31": {
                                                                    "steps": [
                                                                        {
                                                                            "model_dbt_test_model7": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model7",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model7",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model7_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model_dbt_test_model7": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model7",
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
                                                                "chain_34": {
                                                                    "steps": [
                                                                        {
                                                                            "model_dbt_test_model8": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model8",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model8",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model8_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model_dbt_test_model8": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model8",
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
                    "parallel_9": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain_39": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model3": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model3",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model3",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model3_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model3": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model3",
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
                                    "chain_42": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model9": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model9",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model9",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model9_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model9": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model9",
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
                    "model_dbt_test_model10": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-dbt-test-model10",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "model10",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_dbt_test_model10_RESULT",
                    }
                },
                {
                    "model_dbt_test_model10": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-dbt-test-model10",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "model10",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_dbt_test_model10_RESULT",
                    }
                },
                {
                    "parallel_12": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain_49": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model4": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model4",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model4",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model4_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model4": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model4",
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
                                    "chain_50": {
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
                {"init": {"assign": [{"jobId": '${jobId + "-" + suffix}'}]}},
                {
                    "createAndRunBatchJob": {
                        "call": "http.post",
                        "args": {
                            "url": "${batchApiUrl}",
                            "query": {"jobId": "${jobId}"},
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
                                                        '${"dbt --no-write-json " + command + " --target env_execution --project-dir /dbt --profiles-dir /root/.dbt --select " + select}',
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
