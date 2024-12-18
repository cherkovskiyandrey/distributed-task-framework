package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.models.Saga;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.fasterxml.jackson.databind.JavaType;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;

public interface SagaManager {

    void create(Saga sagaContext);

    void track(SagaPipeline context);

    Saga get(UUID sagaId) throws SagaNotFoundException;

    /**
     * @param sagaId
     * @param resultType
     * @param <T>
     * @return saga result or empty when last saga method return void OR saga is already in progress
     * @throws SagaNotFoundException  if saga doesn't exist or completed and was removed by timeout
     * @throws SagaExecutionException if any task (exclude rollback) threw an exception, contains original caused exception
     * @throws CancellationException  if the computation was cancelled
     */
    <T> Optional<T> getSagaResult(UUID sagaId, Class<T> resultType) throws SagaNotFoundException, SagaExecutionException, CancellationException;

    void setOkResult(UUID sagaId, byte[] serializedValue);

    void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType);

    void complete(UUID sagaId);

    boolean isCompleted(UUID sagaId);

    boolean isCanceled(UUID sagaId);

    void cancel(UUID sagaId) throws SagaNotFoundException;

    void forceShutdown(UUID sagaId) throws SagaNotFoundException;
}
