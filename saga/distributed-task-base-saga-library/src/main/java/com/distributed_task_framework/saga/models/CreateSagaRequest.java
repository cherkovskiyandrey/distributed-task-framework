package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.model.TaskId;
import lombok.Builder;
import lombok.Value;

import java.util.UUID;

@Value
@Builder
public class CreateSagaRequest {
    UUID sagaId;
    String name;
    TaskId rootTaskId;
    SagaPipeline lastPipelineContext;
}
