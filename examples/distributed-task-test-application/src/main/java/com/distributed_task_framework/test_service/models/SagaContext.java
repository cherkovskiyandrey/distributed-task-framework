package com.distributed_task_framework.test_service.models;

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
    String sagaMethodTaskName;
    SagaSchemaArguments operationSagaSchemaArguments;

    @Nullable
    String sagaRevertMethodTaskName;

    //todo: think about necessary to use, because there is a problem with deserialization on received side in case
    // exception is deprecated and has been removed in depth of spring lib on new node.
    // In this case we will lost task
    @Deprecated
    @ToString.Exclude
    @Nullable
    Throwable throwable;
    @Nullable
    SagaSchemaArguments revertOperationSagaSchemaArguments;
}
