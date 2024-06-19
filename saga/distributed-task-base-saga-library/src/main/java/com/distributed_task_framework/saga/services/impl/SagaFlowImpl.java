package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.mappers.SagaTrackIdMapper;
import com.distributed_task_framework.saga.models.SagaParsedTrackId;
import com.distributed_task_framework.saga.models.SagaTrackId;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaResultService;
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
    SagaResultService sagaResultService;
    SagaTrackIdMapper sagaTrackIdMapper;
    UUID sagaId;
    TaskId taskId;
    Class<T> resultType;

    @Override
    public void waitCompletion() throws TimeoutException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId);
    }

    @Override
    public void waitCompletion(Duration duration) throws TimeoutException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId, duration);
    }

    @Override
    public Optional<T> get() throws TimeoutException, SagaExecutionException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId);
        return sagaResultService.get(sagaId, resultType);
    }

    @Override
    public Optional<T> get(Duration duration) throws TimeoutException, SagaExecutionException, InterruptedException {
        distributedTaskService.waitCompletionAllWorkflow(taskId, duration);
        return sagaResultService.get(sagaId, resultType);
    }

    @Override
    public boolean isCompleted() {
        //todo
        //throw new UnsupportedOperationException();
        return true;
    }

    @Override
    public SagaTrackId trackId() {
        return sagaTrackIdMapper.map(new SagaParsedTrackId(sagaId, taskId));
    }
}
