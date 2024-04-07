package com.distributed_task_framework.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;

@Value
@Builder
@Jacksonized
public class SagaRevertWithParentInputOnly<PARENT_INPUT, INPUT> {
    @Nullable
    PARENT_INPUT parentInput;
    @Nullable
    INPUT input;
    @Nullable
    Throwable throwable;
}
