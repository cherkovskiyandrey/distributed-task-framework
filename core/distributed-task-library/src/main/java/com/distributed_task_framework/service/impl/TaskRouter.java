package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.BatchRouteMap;
import com.distributed_task_framework.model.BatchRouteRequest;
import com.distributed_task_framework.model.NodeCapacity;
import com.distributed_task_framework.model.NodeTask;
import com.distributed_task_framework.model.NodeTaskActivity;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.PartitionStat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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

        Map<String, Integer> actualTaskLimits = Maps.newHashMap(batchRouteRequest.getActualTaskLimits());
        List<NodeCapacity> availableNodeCapacities = Lists.newArrayList(batchRouteRequest.getAvailableNodeCapacities());
        Map<NodeTask, Integer> nodeTaskToActivity = nodeTaskToActivityAsMap(batchRouteRequest.getNodeTaskActivities());
        Map<Partition, Integer> newAvailablePartitionsToPlan = newTaskBatchesAsMap(batchRouteRequest.getNewAvailablePartitionStatsToPlan());

        List<Partition> availablePartitionsOrder = newAvailablePartitionsToPlan.keySet().stream()
                .sorted()
                .toList();

        Set<UUID> availableActiveNodes = availableNodeCapacities.stream()
                .map(NodeCapacity::getNode)
                .collect(Collectors.toSet());

        normalizeNodeLastUpdate(availableActiveNodes);

        Set<Partition> processed = Sets.newHashSet();
        int cursorIdx = findCursorIdx(availablePartitionsOrder);
        while (processed.size() < availablePartitionsOrder.size()) {
            for (int i = cursorIdx; i < availablePartitionsOrder.size(); ++i) {
                Partition availablePartitionToPlan = availablePartitionsOrder.get(i);
                String taskName = availablePartitionToPlan.getTaskName();
                Integer numberToPlan = newAvailablePartitionsToPlan.getOrDefault(availablePartitionToPlan, 0);
                if (numberToPlan == 0) {
                    processed.add(availablePartitionToPlan);
                    continue;
                }
                if (isLimitReached(availablePartitionToPlan, actualTaskLimits)) {
                    processed.add(availablePartitionToPlan);
                    continue;
                }
                Optional<NodeCapacity> commonCapacityOpt = lookupCapacity(
                        availablePartitionToPlan,
                        availableNodeCapacities,
                        nodeTaskToActivity
                );
                if (commonCapacityOpt.isEmpty()) {
                    processed.add(availablePartitionToPlan);
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

                newAvailablePartitionsToPlan.computeIfPresent(availablePartitionToPlan, (key, oldVal) -> oldVal - 1);
                partitionLimits.compute(availablePartitionToPlan, (key, oldVal) -> oldVal == null ? 1 : oldVal + 1);
                actualTaskLimits.computeIfPresent(taskName, (key, oldVal) ->
                        oldVal == UNLIMITED_PARALLEL_TASKS ? UNLIMITED_PARALLEL_TASKS : oldVal - 1
                );
                partitionCursor = availablePartitionToPlan;
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

    private boolean isLimitReached(Partition partition, Map<String, Integer> taskLimits) {
        return taskLimits.getOrDefault(partition.getTaskName(), UNLIMITED_PARALLEL_TASKS) == 0;
    }

    private Optional<NodeCapacity> lookupCapacity(Partition partitionToPlan,
                                                  List<NodeCapacity> nodeCapacities,
                                                  Map<NodeTask, Integer> nodeTaskToActivity) {
        String taskName = partitionToPlan.getTaskName();
        return nodeCapacities.stream()
                .filter(nodeCapacity -> nodeCapacity.getTaskNames().contains(taskName) &&
                        nodeCapacity.getFreeCapacity() > 0
                )
                .min(Comparator.<NodeCapacity>comparingInt(nodeCapacity -> nodeTaskToActivity.getOrDefault(NodeTask.builder()
                                                        .node(nodeCapacity.getNode())
                                                        .task(taskName)
                                                        .build(),
                                                0
                                        )
                                )
                                .thenComparingInt(NodeCapacity::getBusyCapacity)
                                .thenComparingLong(nodeCapacity ->
                                        nodeLastUpdateNumber.getOrDefault(nodeCapacity.getNode(), 0L)
                                )
                );
    }
}
