package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.model.TaskId;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;
import java.util.UUID;

@Value
@Builder
public class SagaContext {
    UUID sagaId;
    String userName;
    LocalDateTime createdDateUtc;
    @Nullable
    LocalDateTime completedDateUtc;
    LocalDateTime expirationDateUtc;
    TaskId rootTaskId;
    SagaEmbeddedPipelineContext lastPipelineContext;
}
