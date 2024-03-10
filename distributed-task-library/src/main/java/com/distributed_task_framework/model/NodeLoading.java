package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.Value;

import java.util.UUID;

@Value
@Builder(toBuilder = true)
public class NodeLoading {
    UUID node;
    double medianCpuLoading;
}
