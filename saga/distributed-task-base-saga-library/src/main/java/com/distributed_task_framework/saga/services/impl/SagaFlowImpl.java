package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.services.SagaFlow;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class SagaFlowImpl<T> extends SagaFlowWithoutResultImpl implements SagaFlow<T> {
    Class<T> resultType;

    @Override
    public Optional<T> get() throws
        SagaNotFoundException,
        SagaExecutionException,
        InterruptedException,
        TimeoutException,
        SagaCancellationException {
        return get(null);
    }

    @Override
    public Optional<T> get(Duration duration) throws SagaNotFoundException,
        SagaExecutionException,
        InterruptedException,
        TimeoutException,
        SagaCancellationException {
        if (sagaManager.isCompleted(sagaId)) {
            return sagaManager.getSagaResult(sagaId, resultType);
        }
        waitCompletionWorkflow(duration);
        return sagaManager.getSagaResult(sagaId, resultType);
    }
}
