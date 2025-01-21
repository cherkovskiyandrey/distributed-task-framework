package com.distributed_task_framework.saga.services.internal;

import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.models.CreateSagaRequest;
import com.distributed_task_framework.saga.models.Saga;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.fasterxml.jackson.databind.JavaType;

import java.util.Optional;
import java.util.UUID;

public interface SagaManager {

    /**
     * Create new saga.
     *
     * @param createSagaRequest
     * @param sagaSettings
     */
    void create(CreateSagaRequest createSagaRequest, SagaSettings sagaSettings);

    /**
     * Return saga object.
     *
     * @param sagaId
     * @return
     * @throws SagaNotFoundException if saga doesn't exist or completed and was removed by timeout
     */
    Saga get(UUID sagaId) throws SagaNotFoundException;

    /**
     * Return result of saga.
     * If saga doesn't assume return value, empty will be returned.
     *
     * @param sagaId
     * @param resultType
     * @param <T>
     * @return saga result or empty when last saga method return void OR saga is already in progress
     * @throws SagaNotFoundException     if saga doesn't exist or completed and was removed by timeout
     * @throws SagaExecutionException    if any task (exclude rollback) threw an exception, contains original caused exception
     * @throws SagaCancellationException if the computation was cancelled
     */
    <T> Optional<T> getSagaResult(UUID sagaId, Class<T> resultType) throws
        SagaNotFoundException,
        SagaExecutionException,
        SagaCancellationException;

    /**
     * Return saga object if exists.
     *
     * @param sagaId
     * @return
     */
    Optional<Saga> getIfExists(UUID sagaId);

    /**
     * Check whether saga has been completed.
     *
     * @param sagaId
     * @return true if saga is completed
     * @throws SagaNotFoundException if saga doesn't exist or completed and was removed by timeout
     */
    boolean isCompleted(UUID sagaId) throws SagaNotFoundException;

    /**
     * Check whether saga has been canceled.
     *
     * @param sagaId
     * @return
     * @throws SagaNotFoundException
     */
    boolean isCanceled(UUID sagaId) throws SagaNotFoundException;

    /**
     * Track saga if exists: save current context.
     * Don't throw exception if saga doesn't exist.
     *
     * @param sagaPipeline
     */
    void trackIfExists(SagaPipeline sagaPipeline);

    /**
     * Save successful result if saga exists, otherwise - do nothing.
     *
     * @param sagaId
     * @param serializedValue
     */
    void setOkResultIfExists(UUID sagaId, byte[] serializedValue);

    /**
     * Save failed result if saga exists, otherwise - do nothing.
     *
     * @param sagaId
     * @param serializedException
     * @param exceptionType
     */
    void setFailResultIfExists(UUID sagaId, byte[] serializedException, JavaType exceptionType);

    /**
     * Complete saga if saga exists, otherwise - do nothing.
     *
     * @param sagaId
     */
    void completeIfExists(UUID sagaId);

    /**
     * Cancel saga gracefully if exists.
     *
     * @param sagaId
     * @throws SagaNotFoundException
     */
    void cancel(UUID sagaId) throws SagaNotFoundException;

    /**
     * Shutdown saga immediately if exists.
     *
     * @param sagaId
     * @throws SagaNotFoundException
     */
    void forceShutdown(UUID sagaId) throws SagaNotFoundException;
}
