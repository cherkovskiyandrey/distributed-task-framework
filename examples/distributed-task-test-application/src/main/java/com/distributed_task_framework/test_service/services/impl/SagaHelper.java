package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.utils.SagaSchemaArguments;
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

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaHelper {
    TaskSerializer taskSerializer;

    @SneakyThrows
    public <INPUT> SagaPipelineContext buildContextFor(@Nullable SagaPipelineContext parentSagaContext,
                                                       TaskDef<SagaPipelineContext> sagaMethodTaskDef,
                                                       SagaSchemaArguments operationSagaSchemaArguments,
                                                       @Nullable TaskDef<SagaPipelineContext> sagaRevertMethodTaskDef,
                                                       @Nullable SagaSchemaArguments revertOperationSagaSchemaArguments,
                                                       @Nullable INPUT input) {

        if (parentSagaContext == null) {
            parentSagaContext = new SagaPipelineContext();
        }
        parentSagaContext.addSagaContext(SagaContext.builder()
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
}
