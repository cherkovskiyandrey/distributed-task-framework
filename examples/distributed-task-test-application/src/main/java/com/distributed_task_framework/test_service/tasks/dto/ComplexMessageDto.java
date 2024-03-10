package com.distributed_task_framework.test_service.tasks.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.UUID;

@Value
@Builder
@Jacksonized
public class ComplexMessageDto {
    UUID userId;
    int addBalance;
}
