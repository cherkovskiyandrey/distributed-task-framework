package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.Value;

import java.util.UUID;

@Value
@Builder
public class NodeTask {
    UUID node;
    String task;
}
