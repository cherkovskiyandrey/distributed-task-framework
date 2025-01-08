package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaParseObjectException;
import com.distributed_task_framework.saga.models.SagaAction;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.distributed_task_framework.service.TaskSerializer;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaHelper {
    TaskSerializer taskSerializer;

    @SneakyThrows
    public <INPUT> SagaPipeline buildContextFor(@Nullable SagaPipeline parentSagaContext,
                                                TaskDef<SagaPipeline> sagaMethodTaskDef,
                                                SagaSchemaArguments operationSagaSchemaArguments,
                                                @Nullable TaskDef<SagaPipeline> sagaRevertMethodTaskDef,
                                                @Nullable SagaSchemaArguments revertOperationSagaSchemaArguments,
                                                @Nullable INPUT input) {

        if (parentSagaContext == null) {
            parentSagaContext = new SagaPipeline();
        }
        parentSagaContext.addAction(SagaAction.builder()
                .sagaMethodTaskName(sagaMethodTaskDef.getTaskName())
                .operationSagaSchemaArguments(operationSagaSchemaArguments)
                .sagaRevertMethodTaskName(sagaRevertMethodTaskDef != null ? sagaRevertMethodTaskDef.getTaskName() : null)
                .revertOperationSagaSchemaArguments(revertOperationSagaSchemaArguments)
                .serializedInput(taskSerializer.writeValue(input))
                .build()
        );

        return parentSagaContext;
    }

    public Object toMethodArgTypedObject(@Nullable byte[] argument, Parameter parameter) throws IOException {
        if (argument == null) {
            return null;
        }
        JavaType javaType = TypeFactory.defaultInstance().constructType(parameter.getParameterizedType());
        return taskSerializer.readValue(argument, javaType);
    }

    public SagaExecutionException buildExecutionException(@Nullable String exceptionType,
                                                          @Nullable byte[] serializedException) {
        if (exceptionType == null || serializedException == null) {
            return null;
        }

        Throwable rootCause = null;
        String message;
        try {
            var javaType = TypeFactory.defaultInstance().constructFromCanonical(exceptionType);
            rootCause = taskSerializer.readValue(serializedException, javaType);
            message = rootCause.getMessage();
        } catch (Exception exception) {
            log.warn("buildExecutionException(): rootCause can't be deserialized for type=[{}]", exceptionType);
            try {
                message = taskSerializer.readValue(serializedException, Throwable.class).getMessage();
            } catch (IOException e) {
                throw new SagaParseObjectException("Couldn't parse object", e);
            }
        }

        return new SagaExecutionException(message, rootCause, exceptionType);
    }

    public <T> Optional<T> buildObject(byte[] serializedObject, Class<T> resultType) {
        try {
            return Optional.ofNullable(taskSerializer.readValue(serializedObject, resultType));
        } catch (IOException e) {
            throw new SagaParseObjectException("Couldn't parse object", e);
        }
    }

    public boolean isVoidType(Class<?> returnType) {
        return returnType.equals(Void.TYPE);
    }
}
