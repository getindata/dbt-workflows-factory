from dbt_workflows_factory.tasks import SingleTask


def test_task():
    task = SingleTask("task_alias", "task_command", "job_id")
    assert task.get_step() == {
        "task_alias": {
            "call": "subworkflowBatchJob",
            "args": {
                "batchApiUrl": "${batchApiUrl}",
                "command": "task_command",
                "jobId": "job_id",
                "imageUri": "${imageUri}",
            },
            "result": "${job_id}Result",
        }
    }
