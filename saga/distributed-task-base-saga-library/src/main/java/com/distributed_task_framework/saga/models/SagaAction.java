package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.saga.utils.SagaSchemaArguments;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class SagaAction {
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
    @ToString.Exclude
    @Nullable
    byte[] serializedException;
    @Nullable
    SagaSchemaArguments revertOperationSagaSchemaArguments;

    @JsonIgnore
    public boolean hasRevert() {
        return sagaRevertMethodTaskName != null;
    }
}
