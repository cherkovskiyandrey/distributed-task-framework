package com.distributed_task_framework.test_service.models;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.test_service.utils.SagaSchemaArguments;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class SagaContext {
    @ToString.Exclude
    @Nullable
    byte[] serializedInput;
    @ToString.Exclude
    @Nullable
    byte[] serializedOutput;
    TaskDef<SagaPipelineContext> sagaMethodTaskDef;
    SagaSchemaArguments operationSagaSchemaArguments;

    @Nullable
    TaskDef<SagaPipelineContext> sagaRevertMethodTaskDef;
    @ToString.Exclude
    @Nullable
    Throwable throwable;
    @Nullable
    SagaSchemaArguments revertOperationSagaSchemaArguments;
}
