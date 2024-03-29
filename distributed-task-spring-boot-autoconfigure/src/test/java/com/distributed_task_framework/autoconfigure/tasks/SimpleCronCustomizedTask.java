package com.distributed_task_framework.autoconfigure.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskConcurrency;
import com.distributed_task_framework.autoconfigure.annotation.TaskSchedule;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;

@TaskConcurrency(maxParallelInCluster = 1)
@TaskSchedule(cron = "* */10 * * *")
public class SimpleCronCustomizedTask implements Task<Void> {

    @Override
    public TaskDef<Void> getDef() {
        return TaskDef.privateTaskDef("customized-cron", Void.class);
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {

    }

    @Override
    public void onFailure(FailedExecutionContext<Void> failedExecutionContext) {

    }
}
