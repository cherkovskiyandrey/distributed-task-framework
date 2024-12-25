package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.model.TaskId;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;
import java.util.UUID;

@Value
@Builder
public class Saga {
    UUID sagaId;
    String name;
    LocalDateTime createdDateUtc;
    /**
     * Means workflow is completed, including revert flow in case of error.
     */
    @Nullable
    LocalDateTime completedDateUtc;
    /**
     * Expiration date for certain saga.
     */
    LocalDateTime expirationDateUtc;
    /**
     * Flag means this saga is canceled gracefully or hard.
     */
    boolean canceled;
    TaskId rootTaskId;

    @JsonIgnore
    public boolean isCompleted() {
        return completedDateUtc != null;
    }
}
