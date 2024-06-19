package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.model.TaskId;

import java.util.UUID;


public record SagaParsedTrackId(
        UUID sagaId,
        TaskId taskId
) {
}
