package com.distributed_task_framework.test.autoconfigure.tasks;

import com.distributed_task_framework.autoconfigure.annotation.TaskConcurrency;
import com.distributed_task_framework.autoconfigure.annotation.TaskSchedule;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;
import org.springframework.stereotype.Component;

@Component
@TaskConcurrency(maxParallelInCluster = 1)
@TaskSchedule(cron = "* */10 * * *")
public class SimpleCronCustomizedTask implements Task<Void> {
    public static final TaskDef<Void> TASK_DEF = TaskDef.privateTaskDef("customized-cron", Void.class);

    @Override
    public TaskDef<Void> getDef() {
        return TASK_DEF;
    }

    @Override
    public void execute(ExecutionContext<Void> executionContext) throws Exception {

    }

    @Override
    public void onFailure(FailedExecutionContext<Void> failedExecutionContext) {

    }
}
