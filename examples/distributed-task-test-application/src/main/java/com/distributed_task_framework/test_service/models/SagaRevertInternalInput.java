package com.distributed_task_framework.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class SagaRevertInternalInput {
    @Nullable
    Object parentInput;
    @Nullable
    Object input;
    @Nullable
    Object output;
    @Nullable
    Throwable throwable;
}
