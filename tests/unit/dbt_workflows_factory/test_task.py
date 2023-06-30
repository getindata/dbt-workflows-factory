from dbt_workflows_factory.task import SingleTask


def test_task():
    task = SingleTask("job_id", "task_command", "task_alias")
    assert task.create_yml() == {
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
