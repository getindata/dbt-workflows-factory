from dbt_workflows_factory.dbt_workflows_converter import DbtWorkflowsConverter
from dbt_workflows_factory.params import Params


def test_converter():
    converter = DbtWorkflowsConverter(
        params=Params(
            image_uri="image_uri",
            location="region",
            project_id="project_id",
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
                    "model_pipeline_example_orders_run": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-orders-cdb01340-run",
                            "select": "orders",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_orders_run_RESULT",
                    }
                },
                {
                    "model_pipeline_example_orders_test": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-orders-cdb01340-test",
                            "select": "orders",
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
                                                        "jobId": "model-pipeline-example-supplier-part-ea0005d2-run",
                                                        "select": "supplier_parts",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_supplier_parts_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_pipeline_example_supplier_parts_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-supplier-part-ea0005d2-test",
                                                        "select": "supplier_parts",
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
                                                        "jobId": "model-pipeline-example-all-europe-re-7ba0d56c-run",
                                                        "select": "all_europe_region_countries",
                                                        "command": "run",
                                                    },
                                                    "result": "model_pipeline_example_all_europe_region_countries_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_pipeline_example_all_europe_region_countries_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-pipeline-example-all-europe-re-7ba0d56c-test",
                                                        "select": "all_europe_region_countries",
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
                            "jobId": "model-pipeline-example-report-dadfda72-run",
                            "select": "report",
                            "command": "run",
                        },
                        "result": "model_pipeline_example_report_run_RESULT",
                    }
                },
                {
                    "model_pipeline_example_report_test": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-pipeline-example-report-dadfda72-test",
                            "select": "report",
                            "command": "test",
                        },
                        "result": "model_pipeline_example_report_test_RESULT",
                    }
                },
            ]
        },
        "subworkflowBatchJob": {
            "params": ["jobId", "command", "select"],
            "steps": [
                {
                    "init": {
                        "assign": [
                            {"location": "region"},
                            {"projectId": "project_id"},
                            {"jobId": '${jobId + "-" + suffix}'},
                        ]
                    }
                },
                {
                    "createAndRunBatchJob": {
                        "call": "googleapis.batch.v1.projects.locations.jobs.create",
                        "args": {
                            "parent": '${"projects/" + projectId + "/locations/" + location}',
                            "jobId": "${jobId}",
                            "body": {
                                "taskGroups": {
                                    "taskSpec": {
                                        "volumes": [
                                            {"gcs": {"remotePath": "remote_path"}, "mountPath": "key_volume_mount_path"}
                                        ],
                                        "runnables": [
                                            {
                                                "container": {
                                                    "imageUri": "image_uri",
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
            ],
        },
    }


def test_converter_2():
    converter = DbtWorkflowsConverter(
        params=Params(
            image_uri="image_uri",
            location="region",
            project_id="project_id",
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
                    "parallel_7": {
                        "parallel": {
                            "branches": [
                                {
                                    "chain_17": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model1_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model1-14e654e9-run",
                                                        "select": "model1",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model1_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model1_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model1-14e654e9-test",
                                                        "select": "model1",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model1_test_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model2_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model2-31e866d0-run",
                                                        "select": "model2",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model2_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model2_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model2-31e866d0-test",
                                                        "select": "model2",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model2_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_24": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model5_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model5-5094d20f-run",
                                                        "select": "model5",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model5_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model5_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model5-5094d20f-test",
                                                        "select": "model5",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model5_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_27": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model6_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model6-171819c3-run",
                                                        "select": "model6",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model6_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model6_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model6-171819c3-test",
                                                        "select": "model6",
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
                                                                "chain_30": {
                                                                    "steps": [
                                                                        {
                                                                            "model_dbt_test_model7_run": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model7-abfa0155-run",
                                                                                    "select": "model7",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model7_run_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model_dbt_test_model7_test": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model7-abfa0155-test",
                                                                                    "select": "model7",
                                                                                    "command": "test",
                                                                                },
                                                                                "result": "model_dbt_test_model7_test_RESULT",
                                                                            }
                                                                        },
                                                                    ]
                                                                }
                                                            },
                                                            {
                                                                "chain_33": {
                                                                    "steps": [
                                                                        {
                                                                            "model_dbt_test_model8_run": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model8-947ceb56-run",
                                                                                    "select": "model8",
                                                                                    "command": "run",
                                                                                },
                                                                                "result": "model_dbt_test_model8_run_RESULT",
                                                                            }
                                                                        },
                                                                        {
                                                                            "model_dbt_test_model8_test": {
                                                                                "call": "subworkflowBatchJob",
                                                                                "args": {
                                                                                    "jobId": "model-dbt-test-model8-947ceb56-test",
                                                                                    "select": "model8",
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
                                    "chain_38": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model3_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model3-95fd9dac-run",
                                                        "select": "model3",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model3_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model3_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model3-95fd9dac-test",
                                                        "select": "model3",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model3_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_41": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model9_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model9-2f4d8b72-run",
                                                        "select": "model9",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model9_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model9_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model9-2f4d8b72-test",
                                                        "select": "model9",
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
                        "args": {"jobId": "model-dbt-test-model10-60a041ba-run", "select": "model10", "command": "run"},
                        "result": "model_dbt_test_model10_run_RESULT",
                    }
                },
                {
                    "model_dbt_test_model10_test": {
                        "call": "subworkflowBatchJob",
                        "args": {
                            "jobId": "model-dbt-test-model10-60a041ba-test",
                            "select": "model10",
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
                                    "chain_48": {
                                        "steps": [
                                            {
                                                "model_dbt_test_model4_run": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model4-60066808-run",
                                                        "select": "model4",
                                                        "command": "run",
                                                    },
                                                    "result": "model_dbt_test_model4_run_RESULT",
                                                }
                                            },
                                            {
                                                "model_dbt_test_model4_test": {
                                                    "call": "subworkflowBatchJob",
                                                    "args": {
                                                        "jobId": "model-dbt-test-model4-60066808-test",
                                                        "select": "model4",
                                                        "command": "test",
                                                    },
                                                    "result": "model_dbt_test_model4_test_RESULT",
                                                }
                                            },
                                        ]
                                    }
                                },
                                {
                                    "chain_49": {
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
            "params": ["jobId", "command", "select"],
            "steps": [
                {
                    "init": {
                        "assign": [
                            {"location": "region"},
                            {"projectId": "project_id"},
                            {"jobId": '${jobId + "-" + suffix}'},
                        ]
                    }
                },
                {
                    "createAndRunBatchJob": {
                        "call": "googleapis.batch.v1.projects.locations.jobs.create",
                        "args": {
                            "parent": '${"projects/" + projectId + "/locations/" + location}',
                            "jobId": "${jobId}",
                            "body": {
                                "taskGroups": {
                                    "taskSpec": {
                                        "volumes": [
                                            {"gcs": {"remotePath": "remote_path"}, "mountPath": "key_volume_mount_path"}
                                        ],
                                        "runnables": [
                                            {
                                                "container": {
                                                    "imageUri": "image_uri",
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
            ],
        },
    }
