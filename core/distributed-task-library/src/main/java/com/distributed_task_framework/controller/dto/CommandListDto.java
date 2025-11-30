package com.distributed_task_framework.controller.dto;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Value
@Builder(toBuilder = true)
@Jacksonized
public class CommandListDto {
    @Builder.Default
    List<CommandDto> commands = List.of();
}
