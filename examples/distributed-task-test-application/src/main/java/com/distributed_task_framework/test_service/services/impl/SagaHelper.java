package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.models.SagaBuilderContext;
import com.distributed_task_framework.test_service.models.SagaContext;
import com.distributed_task_framework.test_service.models.SagaRevertBuilderContext;
import com.distributed_task_framework.test_service.models.SagaRevertContext;
import com.distributed_task_framework.test_service.models.SagaRevertInternalInput;
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
    public <INPUT> ExecutionContext<SagaContext> buildContextFor(SagaBuilderContext sagaBuilderContext,
                                                                 @Nullable TaskDef<SagaRevertContext> sagaRevertMethodRef,
                                                                 INPUT input) {
        var currentRevertSagaBuilderContext = sagaRevertMethodRef != null ?
                SagaRevertBuilderContext.builder()
                        .revertOperationTaskDef(sagaRevertMethodRef)
                        .serializedArg(taskSerializer.writeValue(SagaRevertInternalInput.builder()
                                .input(input)
                                .build())
                        )
                        .build()
                : null;

        var sagaContext = SagaContext.builder()
                .nextOperationTaskDef(sagaBuilderContext.getNextOperationTaskDef())
                .serializedArg(input != null ? taskSerializer.writeValue(input) : null)
                .currentSagaRevertBuilderContext(currentRevertSagaBuilderContext)
                .build();

        return sagaBuilderContext.hasAffinity() ?
                ExecutionContext.withAffinityGroup(
                        sagaContext,
                        sagaBuilderContext.getAffinityGroup(),
                        sagaBuilderContext.getAffinity()
                )
                : ExecutionContext.simple(sagaContext);
    }

    @SneakyThrows
    public SagaRevertBuilderContext addToSagaRevert(SagaRevertBuilderContext sagaRevertBuilderContext,
                                                    @Nullable Object parentArgument,
                                                    @Nullable Object output,
                                                    @Nullable Throwable throwable) {
        var sagaRevertInternalInput = taskSerializer.readValue(
                sagaRevertBuilderContext.getSerializedArg(),
                SagaRevertInternalInput.class
        );
        sagaRevertInternalInput = sagaRevertInternalInput.toBuilder()
                .parentInput(parentArgument)
                .output(output)
                .throwable(throwable)
                .build();

        return sagaRevertBuilderContext.toBuilder()
                .serializedArg(taskSerializer.writeValue(sagaRevertInternalInput))
                .build();
    }

    public Object toMethodArgTypedObject(@Nullable byte[] argument, Parameter parameter) throws IOException {
        if (argument == null) {
            return null;
        }
        JavaType javaType = TypeFactory.defaultInstance().constructType(parameter.getParameterizedType());
        return taskSerializer.readValue(argument, javaType);
    }
}
