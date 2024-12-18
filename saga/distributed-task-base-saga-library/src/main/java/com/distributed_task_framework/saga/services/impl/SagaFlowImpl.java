package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaManager;
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
    SagaManager sagaManager;
    UUID sagaId;
    Class<T> resultType;

    @Override
    public void waitCompletion() throws SagaNotFoundException, InterruptedException, TimeoutException {
        TaskId taskId = sagaManager.get(sagaId).getRootTaskId();
        distributedTaskService.waitCompletionAllWorkflow(taskId);
    }

    @Override
    public void waitCompletion(Duration duration) throws TimeoutException, InterruptedException {
        TaskId taskId = sagaManager.get(sagaId).getRootTaskId();
        distributedTaskService.waitCompletionAllWorkflow(taskId, duration);
    }

    @Override
    public Optional<T> get() throws TimeoutException, SagaExecutionException, InterruptedException {
        if (sagaManager.isCompleted(sagaId)) {
            return sagaManager.getSagaResult(sagaId, resultType);
        }
        waitCompletion();
        return sagaManager.getSagaResult(sagaId, resultType);
    }

    @Override
    public Optional<T> get(Duration duration) throws TimeoutException, SagaExecutionException, InterruptedException {
        if (sagaManager.isCompleted(sagaId)) {
            return sagaManager.getSagaResult(sagaId, resultType);
        }
        waitCompletion(duration);
        return sagaManager.getSagaResult(sagaId, resultType);
    }

    @Override
    public boolean isCompleted() {
        return sagaManager.isCompleted(sagaId);
    }

    @Override
    public UUID trackId() {
        return sagaId;
    }

    @Override
    public void cancel(boolean gracefully) throws SagaNotFoundException {
        if (gracefully) {
            sagaManager.cancel(sagaId);
        } else {
            sagaManager.forceShutdown(sagaId);
        }
    }
}
