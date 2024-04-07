package com.distributed_task_framework.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;

@Value
@Builder
@Jacksonized
public class SagaRevertInputOnly<INPUT> {
    @Nullable
    INPUT output;
    @Nullable
    Throwable throwable;
}
