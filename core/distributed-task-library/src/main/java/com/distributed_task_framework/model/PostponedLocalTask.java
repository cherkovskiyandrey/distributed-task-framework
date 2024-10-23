package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.Value;

import jakarta.annotation.Nullable;
import java.time.Duration;

@Value
@Builder
public class PostponedLocalTask<T> {
    TaskId taskId;
    TaskDef<T> taskDef;
    ExecutionContext<T> executionContext;
    Duration delay;
    @Nullable
    String overriddenCron;
}
