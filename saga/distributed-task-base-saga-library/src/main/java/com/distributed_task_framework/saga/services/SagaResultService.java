package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.fasterxml.jackson.databind.JavaType;

import java.util.Optional;
import java.util.UUID;

public interface SagaResultService {

    void beginWatching(UUID sagaId);

    <T> Optional<T> get(UUID sagaId, Class<T> resultType) throws SagaExecutionException;

    void setOkResult(UUID sagaId, byte[] serializedValue);

    void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType);
}
