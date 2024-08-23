package com.distributed_task_framework.task.common;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.task.Task;

/**
 * Marker stub class to register remote tasks.
 *
 * @param <T>
 */
public class RemoteStubTask<T> implements Task<T> {
    private final TaskDef<T> taskDef;

    private RemoteStubTask(TaskDef<T> taskDef) {
        this.taskDef = taskDef;
    }

    public static <T> RemoteStubTask<T> stubFor(TaskDef<T> taskDef) {
        return new RemoteStubTask<>(taskDef);
    }

    @Override
    public TaskDef<T> getDef() {
        return taskDef;
    }

    @Override
    public void execute(ExecutionContext<T> executionContext) throws Exception {
        //do nothing here
    }

    @Override
    public void onFailure(FailedExecutionContext<T> failedExecutionContext) {
        //do nothing here
    }
}
