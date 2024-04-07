package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.models.SagaBuilderContext;
import com.distributed_task_framework.test_service.models.SagaContext;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaHelper {
    TaskSerializer taskSerializer;

    @SneakyThrows
    public <INPUT> ExecutionContext<SagaContext> buildContextFor(SagaBuilderContext sagaBuilderContext, INPUT input) {
        var sagaContext = SagaContext.builder()
                .nextOperationTaskDef(sagaBuilderContext.getNextOperationTaskDef())
                .serializedArg(input != null ? taskSerializer.writeValue(input) : null)
                .build();

        return sagaBuilderContext.hasAffinity() ?
                ExecutionContext.withAffinityGroup(
                        sagaContext,
                        sagaBuilderContext.getAffinityGroup(),
                        sagaBuilderContext.getAffinity()
                )
                : ExecutionContext.simple(sagaContext);
    }
}
