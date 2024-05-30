package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.saga.models.SagaTrackId;
import com.distributed_task_framework.saga.services.SagaFlow;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Value
@Builder
public class SagaFlowImpl<T> implements SagaFlow<T> {
    DistributedTaskService distributedTaskService;
    TaskId taskId;

    @Override
    public void waitCompletion() throws TimeoutException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId);
    }

    @Override
    public void waitCompletion(Duration duration) throws TimeoutException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId, duration);
    }

    @Override
    public Optional<T> get() throws TimeoutException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId);
        //todo: read result from local table
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<T> get(Duration duration) throws TimeoutException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId, duration);
        //todo: read result from local table
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCompleted() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SagaTrackId trackId() {
        throw new UnsupportedOperationException();
    }
}
