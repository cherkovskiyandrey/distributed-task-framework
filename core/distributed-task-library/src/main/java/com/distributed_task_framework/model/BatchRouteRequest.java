package com.distributed_task_framework.model;

import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Value
@Builder
public class BatchRouteRequest {
    @Builder.Default
    Set<PartitionStat> newTaskBatches = Set.of();
    @Builder.Default
    Map<String, Integer> actualTaskLimits = Map.of();
    @Builder.Default
    List<NodeTaskActivity> nodeTaskActivities = List.of();
    @Builder.Default
    List<NodeCapacity> nodeCapacities = List.of();
}
