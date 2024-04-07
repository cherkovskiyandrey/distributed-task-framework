package com.distributed_task_framework.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;

@Value
@Builder
@Jacksonized
public class SagaRevert<INPUT, OUTPUT> {
    @Nullable
    INPUT input;
    @Nullable
    OUTPUT output;
    @Nullable
    Throwable throwable;
}
