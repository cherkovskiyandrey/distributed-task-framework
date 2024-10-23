package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.models.SagaContext;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.fasterxml.jackson.databind.JavaType;

import java.util.Optional;
import java.util.UUID;

public interface SagaContextService {

    void create(SagaContext sagaContext);

    void track(SagaEmbeddedPipelineContext context);

    SagaContext get(UUID sagaId) throws SagaNotFoundException;

    <T> Optional<T> getSagaResult(UUID sagaId, Class<T> resultType) throws SagaNotFoundException, SagaExecutionException;

    void setCompleted(UUID sagaId);

    void setOkResult(UUID sagaId, byte[] serializedValue);

    void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType);

    boolean isCompleted(UUID sagaId);
}
