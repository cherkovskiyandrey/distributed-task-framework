package com.distributed_task_framework.controller.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.time.LocalDateTime;
import java.util.UUID;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class CommandDto {
    UUID id;
    String action;
    String appName;
    String taskName;
    LocalDateTime createdDateUtc;
    byte[] body;
}
