package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

import java.util.UUID;

@Value
@Builder
public class JoinTaskExecution {
    UUID taskId;
    @ToString.Exclude
    byte[] joinedMessage;
}
