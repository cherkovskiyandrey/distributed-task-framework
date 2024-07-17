package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.services.SagaContextService;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.service.DistributedTaskService;
import lombok.Builder;
import lombok.Value;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Value
@Builder
public class SagaFlowImpl<T> implements SagaFlow<T> {
    DistributedTaskService distributedTaskService;
    SagaContextService sagaContextService;
    UUID sagaId;
    Class<T> resultType;

    @Override
    public void waitCompletion() throws SagaNotFoundException, InterruptedException, TimeoutException {
        TaskId taskId = sagaContextService.get(sagaId).getRootTaskId();
        distributedTaskService.waitCompletionAllWorkflow(taskId);
    }

    @Override
    public void waitCompletion(Duration duration) throws TimeoutException, InterruptedException {
        TaskId taskId = sagaContextService.get(sagaId).getRootTaskId();
        distributedTaskService.waitCompletionAllWorkflow(taskId, duration);
    }

    @Override
    public Optional<T> get() throws TimeoutException, SagaExecutionException, InterruptedException {
        waitCompletion();
        return sagaContextService.getSagaResult(sagaId, resultType);
    }

    @Override
    public Optional<T> get(Duration duration) throws TimeoutException, SagaExecutionException, InterruptedException {
        waitCompletion(duration);
        return sagaContextService.getSagaResult(sagaId, resultType);
    }

    @Override
    public boolean isCompleted() {
        //todo
        //throw new UnsupportedOperationException();
        return true;
    }

    @Override
    public UUID trackId() {
        return sagaId;
    }
}
