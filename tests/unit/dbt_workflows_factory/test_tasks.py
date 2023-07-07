from dbt_workflows_factory.tasks import CustomStep, NodeStep, TaskCommand


def test_task():
    task = NodeStep("task_alias", "task_select", TaskCommand.RUN, "my_job_id")
    assert task.get_step() == {
        "task_alias": {
            "call": "subworkflowBatchJob",
            "args": {
                "batchApiUrl": "${batchApiUrl}",
                "select": "task_select",
                "command": "run",
                "jobId": '${"my_job_id_" + string(int(sys.now))}',
                "imageUri": "${imageUri}",
            },
            "result": "task_alias_RESULT",
        }
    }


def test_custom_task():
    task = CustomStep({"init": {"assign": [{"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'}]}})
    assert task.get_step() == {"init": {"assign": [{"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'}]}}
