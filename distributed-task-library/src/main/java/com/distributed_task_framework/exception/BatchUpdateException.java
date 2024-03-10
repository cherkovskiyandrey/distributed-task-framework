package com.distributed_task_framework.exception;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.List;
import java.util.UUID;

@Value
@EqualsAndHashCode(callSuper = true)
@Builder
public class BatchUpdateException extends RuntimeException {
    @Builder.Default
    List<UUID> unknownTaskIds = List.of();
    @Builder.Default
    List<UUID> optimisticLockTaskIds = List.of();
}
