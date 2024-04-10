package com.distributed_task_framework.test_service.models;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class SagaRevertContext {
    byte[] serializedArg;
}
