package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.test_service.models.SagaTrackId;
import com.distributed_task_framework.test_service.services.SagaFlow;
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
    public void waitCompletion() throws TimeoutException {
        //todo: implement method + think about how to return task result?
        distributedTaskService.waitCompletion(taskId.getWorkflowId());
    }

    @Override
    public void waitCompletion(Duration duration) throws TimeoutException {
        distributedTaskService.waitCompletion(taskId.getWorkflowId(), duration);
    }

    @Override
    public Optional<T> get() throws TimeoutException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<T> get(Duration duration) throws TimeoutException {
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
