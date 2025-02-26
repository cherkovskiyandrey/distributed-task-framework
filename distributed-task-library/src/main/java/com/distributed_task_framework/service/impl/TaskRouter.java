package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

import static com.distributed_task_framework.settings.CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
public class TaskRouter {
    final Map<UUID, Long> nodeLastUpdateNumber;
    Partition partitionCursor;

    public TaskRouter() {
        this.nodeLastUpdateNumber = Maps.newConcurrentMap();
        this.partitionCursor = null;
    }

    @VisibleForTesting
    public TaskRouter(Map<UUID, Long> nodeLastUpdateNumber,
                      Partition partitionCursor) {
        this.nodeLastUpdateNumber = nodeLastUpdateNumber;
        this.partitionCursor = partitionCursor;
    }

    private int findCursorIdx(List<Partition> order) {
        if (partitionCursor == null) {
            return 0;
        }
        int i = Collections.binarySearch(order, partitionCursor);
        if (i < 0) {
            i = Math.abs(i) - 1;
            return i % order.size();
        }
        return (i + 1) % order.size();
    }

    public BatchRouteMap batchRoute(BatchRouteRequest batchRouteRequest) {
        Map<Partition, Integer> partitionLimits = Maps.newHashMap();
        Table<String, UUID, Integer> taskNameNodeQuota = HashBasedTable.create();

        Map<String, Integer> actualClusterTaskLimits = Maps.newHashMap(batchRouteRequest.getActualClusterTaskLimits());
        Map<String, Integer> nodeTaskLimits = batchRouteRequest.getNodeTaskLimits();
        List<NodeCapacity> nodeCapacities = Lists.newArrayList(batchRouteRequest.getNodeCapacities());
        Map<NodeTask, Integer> nodeTaskToActivity = nodeTaskToActivityAsMap(batchRouteRequest.getNodeTaskActivities());
        Map<Partition, Integer> newTaskBatches = newTaskBatchesAsMap(batchRouteRequest.getPartitionStatsToPlan());

        List<Partition> order = newTaskBatches.keySet().stream()
                .sorted()
                .toList();

        Set<UUID> activeNodes = nodeCapacities.stream()
                .map(NodeCapacity::getNode)
                .collect(Collectors.toSet());

        normalizeNodeLastUpdate(activeNodes);

        Set<Partition> processed = Sets.newHashSet();
        int cursorIdx = findCursorIdx(order);
        while (processed.size() < order.size()) {
            for (int i = cursorIdx; i < order.size(); ++i) {
                Partition partitionToPlan = order.get(i);
                String taskName = partitionToPlan.getTaskName();
                Integer numberToPlan = newTaskBatches.getOrDefault(partitionToPlan, 0);
                if (numberToPlan == 0) {
                    processed.add(partitionToPlan);
                    continue;
                }
                if (isClusterTaskLimitReached(partitionToPlan, actualClusterTaskLimits)) {
                    processed.add(partitionToPlan);
                    continue;
                }
                Optional<NodeCapacity> commonCapacityOpt = lookupCapacity(
                        partitionToPlan,
                        nodeCapacities,
                        nodeTaskToActivity,
                        nodeTaskLimits
                );
                if (commonCapacityOpt.isEmpty()) {
                    processed.add(partitionToPlan);
                    continue;
                }

                NodeCapacity nodeCapacity = commonCapacityOpt.get();
                nodeCapacity.busyOnlyOne();

                NodeTask affectedNodeTask = NodeTask.builder()
                        .node(nodeCapacity.getNode())
                        .task(taskName)
                        .build();
                nodeTaskToActivity.compute(affectedNodeTask, (key, oldVal) -> oldVal == null ? 1 : oldVal + 1);

                Integer currentQuota = taskNameNodeQuota.get(taskName, nodeCapacity.getNode());
                taskNameNodeQuota.put(taskName, nodeCapacity.getNode(), currentQuota == null ? 1 : currentQuota + 1);

                newTaskBatches.computeIfPresent(partitionToPlan, (key, oldVal) -> oldVal - 1);
                partitionLimits.compute(partitionToPlan, (key, oldVal) -> oldVal == null ? 1 : oldVal + 1);
                actualClusterTaskLimits.computeIfPresent(taskName, (key, oldVal) ->
                        oldVal == UNLIMITED_PARALLEL_TASKS ? UNLIMITED_PARALLEL_TASKS : oldVal - 1
                );
                partitionCursor = partitionToPlan;
                nodeLastUpdateNumber.put(nodeCapacity.getNode(), currentUpdateNumber() + 1);
            }
            cursorIdx = 0;
        }

        return BatchRouteMap.builder()
                .partitionLimits(partitionLimits)
                .taskNameNodeQuota(taskNameNodeQuota)
                .build();
    }

    @VisibleForTesting
    Optional<Partition> affinityGroupTaskNameEntityCursor() {
        return Optional.ofNullable(partitionCursor);
    }

    @VisibleForTesting
    Optional<UUID> lastUpdatedNode() {
        return nodeLastUpdateNumber.entrySet().stream()
                .max(Comparator.comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey);

    }

    private long currentUpdateNumber() {
        return nodeLastUpdateNumber.values()
                .stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0);
    }

    private void normalizeNodeLastUpdate(Set<UUID> activeNodes) {
        var inactiveNodeIds = Sets.newHashSet(Sets.difference(nodeLastUpdateNumber.keySet(), activeNodes));
        inactiveNodeIds.forEach(nodeLastUpdateNumber::remove);

        Optional<Long> minValOpt = nodeLastUpdateNumber.entrySet().stream()
                .min(Comparator.comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getValue);
        if (minValOpt.isEmpty() || minValOpt.get() == 0) {
            return;
        }

        long minVal = minValOpt.get();
        nodeLastUpdateNumber.forEach((key, value) -> nodeLastUpdateNumber.put(key, value - minVal));
    }

    private Map<Partition, Integer> newTaskBatchesAsMap(Set<PartitionStat> newTaskBatches) {
        return newTaskBatches.stream()
                .collect(Collectors.toMap(
                        stat -> Partition.builder()
                                .affinityGroup(stat.getAffinityGroup())
                                .taskName(stat.getTaskName())
                                .build(),
                        PartitionStat::getNumber
                ));
    }

    private Map<NodeTask, Integer> nodeTaskToActivityAsMap(List<NodeTaskActivity> nodeTaskActivities) {
        return nodeTaskActivities.stream()
                .collect(Collectors.toMap(
                        nodeTaskActivity -> NodeTask.builder()
                                .node(nodeTaskActivity.getNode())
                                .task(nodeTaskActivity.getTask())
                                .build(),
                        NodeTaskActivity::getNumber
                ));
    }

    private boolean isClusterTaskLimitReached(Partition partition, Map<String, Integer> taskLimits) {
        return taskLimits.getOrDefault(partition.getTaskName(), UNLIMITED_PARALLEL_TASKS) == 0;
    }

    private Optional<NodeCapacity> lookupCapacity(Partition partitionToPlan,
                                                  List<NodeCapacity> nodeCapacities,
                                                  Map<NodeTask, Integer> nodeTaskToActivity,
                                                  Map<String, Integer> actualNodeTaskLimits) {
        String taskName = partitionToPlan.getTaskName();
        var nodeTaskLimit = actualNodeTaskLimits.getOrDefault(taskName, UNLIMITED_PARALLEL_TASKS);
        return nodeCapacities.stream()
                //apply node capacity limit
                .filter(nodeCapacity -> nodeCapacity.getTaskNames().contains(taskName) && nodeCapacity.getFreeCapacity() > 0)
                //apply node task capacity limit
                .filter(nodeCapacity -> {
                            if (nodeTaskLimit == UNLIMITED_PARALLEL_TASKS) {
                                return true;
                            }
                            var busyNumber = nodeTaskToActivity.getOrDefault(NodeTask.builder()
                                            .node(nodeCapacity.getNode())
                                            .task(taskName)
                                            .build(),
                                    0
                            );
                            return busyNumber < nodeTaskLimit;
                        }
                )
                //1. min certain task number
                .min(Comparator.<NodeCapacity>comparingInt(nodeCapacity -> nodeTaskToActivity.getOrDefault(NodeTask.builder()
                                                        .node(nodeCapacity.getNode())
                                                        .task(taskName)
                                                        .build(),
                                                0
                                        )
                                )
                                //2. least busy node
                                .thenComparingInt(NodeCapacity::getBusyCapacity)
                                //3. the earliest updated node
                                .thenComparingLong(nodeCapacity ->
                                        nodeLastUpdateNumber.getOrDefault(nodeCapacity.getNode(), 0L)
                                )
                );
    }
}
