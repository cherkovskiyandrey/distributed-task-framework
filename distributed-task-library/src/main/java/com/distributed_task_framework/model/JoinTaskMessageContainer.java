package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

@Value
@Builder
@Jacksonized
public class JoinTaskMessageContainer {
    @Builder.Default
    List<byte[]> rawMessages = List.of();
}
