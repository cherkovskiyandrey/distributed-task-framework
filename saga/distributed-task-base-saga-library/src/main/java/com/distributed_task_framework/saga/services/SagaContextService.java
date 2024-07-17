package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.models.SagaContext;
import com.fasterxml.jackson.databind.JavaType;

import java.util.Optional;
import java.util.UUID;

public interface SagaContextService {

    void create(UUID sagaId, TaskId taskId);

    SagaContext get(UUID sagaId) throws SagaNotFoundException;

    <T> Optional<T> getSagaResult(UUID sagaId, Class<T> resultType) throws SagaNotFoundException, SagaExecutionException;

    void setCompleted(UUID sagaId);

    void setOkResult(UUID sagaId, byte[] serializedValue);

    void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType);
}
