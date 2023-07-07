from dbt_workflows_factory.tasks import CustomTask, NodeTask, TaskCommand


def test_task():
    task = NodeTask("task_alias", "task_select", TaskCommand.RUN, "my_job_id")
    assert task.get_step() == {
        "task_alias": {
            "call": "subworkflowBatchJob",
            "args": {
                "batchApiUrl": "${batchApiUrl}",
                "select": "task_select",
                "command": "run",
                "jobId": "my_job_id",
                "imageUri": "${imageUri}",
            },
            "result": "my_job_id_RESULT",
        }
    }


def test_custom_task():
    task = CustomTask({"init": {"assign": [{"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'}]}})
    assert task.get_step() == {"init": {"assign": [{"projectId": '${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}'}]}}
