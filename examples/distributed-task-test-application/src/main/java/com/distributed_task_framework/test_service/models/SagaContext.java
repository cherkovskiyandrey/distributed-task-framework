package com.distributed_task_framework.test_service.models;

import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.annotation.Nullable;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class SagaContext {
    @Nullable
    byte[] serializedArg;
    @Nullable
    TaskDef<SagaContext> nextOperationTaskDef;
}
