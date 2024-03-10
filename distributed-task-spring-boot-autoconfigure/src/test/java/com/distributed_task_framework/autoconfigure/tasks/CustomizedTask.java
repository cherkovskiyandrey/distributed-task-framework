package com.distributed_task_framework.autoconfigure.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskBackoffRetryPolicy;
import com.distributed_task_framework.autoconfigure.annotation.TaskConcurrency;
import com.distributed_task_framework.autoconfigure.annotation.TaskDltEnable;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.autoconfigure.annotation.TaskSchedule;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;

@TaskBackoffRetryPolicy(
        initialDelay = "PT1m",
        maxRetries = 100
)
@TaskConcurrency(maxParallelInCluster = 10)
@TaskDltEnable
@TaskSchedule(cron = "* */10 * * *")
@TaskExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
public class CustomizedTask implements Task<Void> {

    @Override
    public TaskDef<Void> getDef() {
        return TaskDef.privateTaskDef("customized", Void.class);
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {
    }

    @Override
    public void onFailure(FailedExecutionContext<Void> failedExecutionContext) {
    }
}
