package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.service.DistributedTaskService;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Data
@FieldDefaults(level = AccessLevel.PROTECTED)
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder(builderMethodName = "builderWithoutResult")
public class SagaFlowWithoutResultImpl implements SagaFlowWithoutResult {
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    UUID sagaId;

    @Override
    public void waitCompletion() throws SagaNotFoundException, InterruptedException, TimeoutException {
        TaskId taskId = sagaManager.get(sagaId).getRootTaskId();
        distributedTaskService.waitCompletionAllWorkflow(taskId);
    }

    @Override
    public void waitCompletion(Duration duration) throws SagaNotFoundException, InterruptedException, TimeoutException {
        TaskId taskId = sagaManager.get(sagaId).getRootTaskId();
        distributedTaskService.waitCompletionAllWorkflow(taskId, duration);
    }

    @Override
    public boolean isCompleted() throws SagaNotFoundException {
        return sagaManager.isCompleted(sagaId);
    }

    @Override
    public boolean isCanceled() throws SagaNotFoundException {
        return sagaManager.isCanceled(sagaId);
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
