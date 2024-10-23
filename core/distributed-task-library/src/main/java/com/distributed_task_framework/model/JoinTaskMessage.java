package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.lang.Nullable;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class JoinTaskMessage<T> {
    TaskId taskId;
    @Nullable
    T message;
}
