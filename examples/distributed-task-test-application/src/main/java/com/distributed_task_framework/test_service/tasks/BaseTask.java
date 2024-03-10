package com.distributed_task_framework.test_service.tasks;

import lombok.extern.slf4j.Slf4j;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.task.Task;

@Slf4j
public abstract class BaseTask<T> implements Task<T> {

    @Override
    public boolean onFailureWithResult(FailedExecutionContext<T> failedExecutionContext) {
        log.error("onFailure(): {}", failedExecutionContext);
        return false;
    }
}
