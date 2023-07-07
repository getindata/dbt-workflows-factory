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
                    "model_pipeline_example_orders_run": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-orders-run",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "orders",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_orders_run_RESULT",
                    }
                },
                {
                    "model_pipeline_example_orders_test": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-orders-test",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "orders",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_pipeline_example_orders_test_RESULT",
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
                                                "model_pipeline_example_supplier_parts_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-supplier-parts-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "supplier_parts",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_supplier_parts_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_pipeline_example_supplier_parts_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-supplier-parts-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "supplier_parts",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_pipeline_example_supplier_parts_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_8": {
                                        "steps": [
                                            {
                                                "model_pipeline_example_all_europe_region_countries_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-all-europe-region-coun-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "all_europe_region_countries",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_all_europe_region_countries_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_pipeline_example_all_europe_region_countries_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-all-europe-region-coun-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "all_europe_region_countries",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_pipeline_example_all_europe_region_countries_test_RESULT",
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
                    "model_pipeline_example_report_run": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-report-run",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "report",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_report_run_RESULT",
                    }
                },
                {
                    "model_pipeline_example_report_test": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-report-test",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "report",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_pipeline_example_report_test_RESULT",
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
                    "parallel_7": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain_18": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model1_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model1-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model1",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model1_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model1_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model1-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model1",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model1_test_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model2_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model2-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model2",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model2_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model2_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model2-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model2",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model2_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_25": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model5_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model5-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model5",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model5_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model5_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model5-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model5",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model5_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_28": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model6_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model6-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model6",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model6_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model6_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model6-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model6",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model6_test_RESULT",
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
                                                                            "model_dbt_test_model7_run": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model7-run",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model7",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model7_run_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model_dbt_test_model7_test": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model7-test",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model7",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "test",
                                                                                },
                                                                                "result": "model_dbt_test_model7_test_RESULT",
                                                                            }
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "chain_34": {
                                                                    "steps": [
                                                                        {
                                                                            "model_dbt_test_model8_run": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model8-run",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model8",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model8_run_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model_dbt_test_model8_test": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model8-test",
                                                                                    "batchApiUrl": "${batchApiUrl}",
                                                                                    "select": "model8",
                                                                                    "imageUri": "${imageUri}",
                                                                                    "command": "test",
                                                                                },
                                                                                "result": "model_dbt_test_model8_test_RESULT",
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
                                                "model_dbt_test_model3_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model3-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model3",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model3_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model3_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model3-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model3",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model3_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_42": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model9_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model9-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model9",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model9_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model9_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model9-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model9",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model9_test_RESULT",
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
                    "model_dbt_test_model10_run": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-dbt-test-model10-run",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "model10",
                            "imageUri": "${imageUri}",
                            "command": "run",
                        },
                        "result": "model_dbt_test_model10_run_RESULT",
                    }
                },
                {
                    "model_dbt_test_model10_test": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-dbt-test-model10-test",
                            "batchApiUrl": "${batchApiUrl}",
                            "select": "model10",
                            "imageUri": "${imageUri}",
                            "command": "test",
                        },
                        "result": "model_dbt_test_model10_test_RESULT",
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
                                                "model_dbt_test_model4_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model4-run",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model4",
                                                        "imageUri": "${imageUri}",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model4_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model4_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model4-test",
                                                        "batchApiUrl": "${batchApiUrl}",
                                                        "select": "model4",
                                                        "imageUri": "${imageUri}",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model4_test_RESULT",
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
