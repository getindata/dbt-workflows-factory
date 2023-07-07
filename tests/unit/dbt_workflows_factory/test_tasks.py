from dbt_workflows_factory.tasks import SingleTask, TaskCommand


def test_task():
    task = SingleTask("task_alias", "task_select", TaskCommand.RUN, "my_job_id")
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
