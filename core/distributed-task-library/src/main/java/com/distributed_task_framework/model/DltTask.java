package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DltTask<T> {
    TaskId taskId;
    ExecutionContext<T> executionContext;
    String errorMessage;
    int errorCode;
}
