package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
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
    String sagaMethodTaskName;
    SagaSchemaArguments operationSagaSchemaArguments;

    @Nullable
    String sagaRevertMethodTaskName;
    @Nullable
    String exceptionType;
    @Nullable
    byte[] serializedException;
    @Nullable
    SagaSchemaArguments revertOperationSagaSchemaArguments;
}
