package com.distributed_task_framework.model;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import lombok.Builder;
import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
@Builder
public class BatchRouteMap {
    @Builder.Default
    Map<Partition, Integer> partitionLimits = Map.of();
    @Builder.Default
    Table<String, UUID, Integer> taskNameNodeQuota = ImmutableTable.of();
}
