package com.distributed_task_framework.test_service.models;

import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class SagaRevertBuilderContext {
    byte[] serializedArg;
    TaskDef<SagaRevertContext> revertOperationTaskDef;
}
