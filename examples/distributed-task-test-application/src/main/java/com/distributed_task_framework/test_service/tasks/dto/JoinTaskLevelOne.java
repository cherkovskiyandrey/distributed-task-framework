package com.distributed_task_framework.test_service.tasks.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class JoinTaskLevelOne {
    String message;
}
