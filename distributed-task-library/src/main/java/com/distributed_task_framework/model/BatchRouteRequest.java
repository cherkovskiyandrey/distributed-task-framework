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
    Set<PartitionStat> partitionStatsToPlan = Set.of();
    @Builder.Default
    Map<String, Integer> actualClusterTaskLimits = Map.of();
    @Builder.Default
    Map<String, Integer> nodeTaskLimits = Map.of();
    @Builder.Default
    List<NodeTaskActivity> nodeTaskActivities = List.of();
    @Builder.Default
    List<NodeCapacity> nodeCapacities = List.of();
}
