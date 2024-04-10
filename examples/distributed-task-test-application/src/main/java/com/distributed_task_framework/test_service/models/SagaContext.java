package com.distributed_task_framework.test_service.models;

import com.distributed_task_framework.model.TaskDef;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class SagaContext {
    @Nullable
    byte[] serializedArg;
    @Nullable
    TaskDef<SagaContext> nextOperationTaskDef;
    @Nullable
    SagaRevertBuilderContext currentSagaRevertBuilderContext;
    @Singular
    List<SagaRevertBuilderContext> sagaRevertBuilderContexts;
}
