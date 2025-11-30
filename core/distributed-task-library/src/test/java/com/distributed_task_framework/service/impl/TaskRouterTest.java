package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.BatchRouteMap;
import com.distributed_task_framework.model.BatchRouteRequest;
import com.distributed_task_framework.model.NodeCapacity;
import com.distributed_task_framework.model.NodeTaskActivity;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.PartitionStat;
import com.distributed_task_framework.settings.CommonSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TaskRouterTest {
    private static final List<UUID> NODES = IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).toList();
    private static final String AFG_1 = "AFG_1";
    private static final String AFG_2 = "AFG_2";
    private static final String TASK_NAME_1 = "TASK_NAME_1";
    private static final String TASK_NAME_2 = "TASK_NAME_2";
    private static final Set<String> SUPPORTED_TASKS = Set.of(TASK_NAME_1, TASK_NAME_2);

    private static Arguments shouldRouteFair() {
        return Arguments.of(
            InputTestData.withoutState(BatchRouteRequest.builder()
                .newAvailablePartitionStatsToPlan(Set.of(stat(AFG_1, TASK_NAME_1, 10)))
                .actualClusterTaskLimits(Map.of(TASK_NAME_1, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS))
                .availableNodeCapacities(capacityForAllNodes(100))
                .build()
            ),
            ExpectedOutputData.withoutInternalState(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(partition(AFG_1, TASK_NAME_1), 10))
                    .taskNameNodeQuota(fairQuotaDistribution(TASK_NAME_1, 1))
                    .build()
            )
        );
    }

    private static Arguments shouldRouteBaseOnCurrentLoadWhenNotEvenly() {
        var nodeTaskActivities = List.of(new NodeTaskActivity(NODES.get(0), TASK_NAME_1, 10));
        return Arguments.of(
            InputTestData.withoutState(BatchRouteRequest.builder()
                .newAvailablePartitionStatsToPlan(Set.of(stat(AFG_1, TASK_NAME_1, 18))) //9(available nodes)*2 = 18
                .actualClusterTaskLimits(Map.of(TASK_NAME_1, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS))
                .nodeTaskActivities(nodeTaskActivities)
                .availableNodeCapacities(capacityForAllNodes(100, nodeTaskActivities))
                .build()
            ),
            ExpectedOutputData.withoutInternalState(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(partition(AFG_1, TASK_NAME_1), 18))
                    .taskNameNodeQuota(fairQuotaDistribution(TASK_NAME_1, 2, Set.of(NODES.get(0))))
                    .build()
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static Arguments shouldRouteBaseOnCurrentLoadWhenTaskEvenlyAndAllTasksUnevenly() {
        List<NodeTaskActivity> nodeTaskActivities = all(
            fairActivityDistribution(TASK_NAME_1, 10),
            List.of(new NodeTaskActivity(NODES.get(0), TASK_NAME_2, 10))
        );

        return Arguments.of(
            InputTestData.withoutState(BatchRouteRequest.builder()
                .newAvailablePartitionStatsToPlan(Set.of(stat(AFG_1, TASK_NAME_1, 19))) //9 + 1 + 9
                .actualClusterTaskLimits(Map.of(
                        TASK_NAME_1, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS,
                        TASK_NAME_2, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS
                    )
                )
                .nodeTaskActivities(nodeTaskActivities)
                .availableNodeCapacities(capacityForAllNodes(100, nodeTaskActivities))
                .build()
            ),
            ExpectedOutputData.withoutInternalState(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(partition(AFG_1, TASK_NAME_1), 19))
                    .taskNameNodeQuota(all(
                            ImmutableTable.of(TASK_NAME_1, NODES.get(0), 1),
                            fairQuotaDistribution(TASK_NAME_1, 2, Set.of(NODES.get(0)))
                        )
                    )
                    .build()
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static Arguments shouldRouteBaseOnEarliestAssignedWhenTaskEvenly() {
        List<NodeTaskActivity> nodeTaskActivities = all(
            fairActivityDistribution(TASK_NAME_1, 10),
            fairActivityDistribution(TASK_NAME_2, 10)
        );

        return Arguments.of(
            InputTestData.withNodeLastUpdateNumber(
                updatedNumber(9, 8, 7, 6, 5, 0, 4, 3, 2, 1),
                BatchRouteRequest.builder()
                    .newAvailablePartitionStatsToPlan(Set.of(stat(AFG_1, TASK_NAME_1, 1)))
                    .actualClusterTaskLimits(Map.of(
                            TASK_NAME_1, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS,
                            TASK_NAME_2, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS
                        )
                    )
                    .nodeTaskActivities(nodeTaskActivities)
                    .availableNodeCapacities(capacityForAllNodes(100, nodeTaskActivities))
                    .build()
            ),
            ExpectedOutputData.withoutInternalState(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(partition(AFG_1, TASK_NAME_1), 1))
                    .taskNameNodeQuota(ImmutableTable.of(TASK_NAME_1, NODES.get(5), 1))
                    .build()
            )
        );
    }

    private static Arguments shouldRouteBaseOnLastAffinityGroupAndTaskAssigned() {
        //leave only one place
        List<NodeTaskActivity> nodeTaskActivities = all(
            List.of(new NodeTaskActivity(NODES.get(0), TASK_NAME_1, 9)),
            fairActivityDistribution(TASK_NAME_1, 10, Set.of(NODES.get(0)))
        );

        return Arguments.of(
            InputTestData.withPartitionCursor(
                partition(AFG_1, TASK_NAME_1),
                BatchRouteRequest.builder()
                    .newAvailablePartitionStatsToPlan(Set.of(
                            stat(AFG_1, TASK_NAME_1, 1),
                            stat(AFG_2, TASK_NAME_2, 1)
                        )
                    )
                    .nodeTaskActivities(nodeTaskActivities)
                    .actualClusterTaskLimits(Map.of(TASK_NAME_1, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS))
                    .availableNodeCapacities(capacityForAllNodes(10, nodeTaskActivities))
                    .build()
            ),
            ExpectedOutputData.withoutInternalState(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(partition(AFG_2, TASK_NAME_2), 1))
                    .taskNameNodeQuota(singleQuota(TASK_NAME_2, NODES.get(0), 1))
                    .build()
            )
        );
    }

    private static Arguments shouldRouteBaseOnClusterTaskLimit() {
        //leave only one place
        List<NodeTaskActivity> nodeTaskActivities = all(
            fairActivityDistribution(TASK_NAME_1, 1)
        );

        return Arguments.of(
            InputTestData.withoutState(
                BatchRouteRequest.builder()
                    .newAvailablePartitionStatsToPlan(Set.of(
                            stat(AFG_1, TASK_NAME_1, 10),
                            stat(AFG_2, TASK_NAME_2, 9)
                        )
                    )
                    .actualClusterTaskLimits(Map.of(
                            TASK_NAME_1, 1,
                            TASK_NAME_2, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS
                        )
                    )
                    .nodeTaskLimits(Map.of(TASK_NAME_1, 3))
                    .nodeTaskActivities(nodeTaskActivities)
                    .availableNodeCapacities(capacityForAllNodes(10, nodeTaskActivities))
                    .build()
            ),
            ExpectedOutputData.withoutInternalState(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(
                            partition(AFG_1, TASK_NAME_1), 1,
                            partition(AFG_2, TASK_NAME_2), 9
                        )
                    )
                    .taskNameNodeQuota(all(
                            singleQuota(TASK_NAME_1, NODES.get(0), 1),
                            fairQuotaDistribution(TASK_NAME_2, 1, Set.of(NODES.get(0)))
                        )
                    )
                    .build()
            )
        );
    }

    private static Arguments shouldRouteBaseOnNodeTaskLimit() {
        //leave only one place
        List<NodeTaskActivity> nodeTaskActivities = all(
            fairActivityDistribution(TASK_NAME_1, 10)
        );

        return Arguments.of(
            InputTestData.withoutState(
                BatchRouteRequest.builder()
                    .newAvailablePartitionStatsToPlan(Set.of(
                            stat(AFG_1, TASK_NAME_1, 100),
                            stat(AFG_2, TASK_NAME_2, 100)
                        )
                    )
                    .actualClusterTaskLimits(Map.of(
                            TASK_NAME_1, 100,
                            TASK_NAME_2, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS
                        )
                    )
                    .nodeTaskLimits(Map.of(TASK_NAME_1, 15))
                    .nodeTaskActivities(nodeTaskActivities)
                    .availableNodeCapacities(capacityForAllNodes(30, nodeTaskActivities))
                    .build()
            ),
            ExpectedOutputData.withoutInternalState(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(
                            partition(AFG_1, TASK_NAME_1), 50,
                            partition(AFG_2, TASK_NAME_2), 100
                        )
                    )
                    .taskNameNodeQuota(all(
                            fairQuotaDistribution(TASK_NAME_1, 5),
                            fairQuotaDistribution(TASK_NAME_2, 10)
                        )
                    )
                    .build()
            )
        );
    }

    private static Arguments shouldCalculateCursorAndAffinityGroupTaskNameCorrectly() {
        return Arguments.of(
            InputTestData.withoutState(
                BatchRouteRequest.builder()
                    .newAvailablePartitionStatsToPlan(Set.of(
                            stat(AFG_1, TASK_NAME_1, 10),
                            stat(AFG_2, TASK_NAME_2, 9)
                        )
                    )
                    .actualClusterTaskLimits(Map.of(
                            TASK_NAME_1, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS,
                            TASK_NAME_2, CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS
                        )
                    )
                    .availableNodeCapacities(capacityForAllNodes(20))
                    .build()
            ),
            new ExpectedOutputData(
                BatchRouteMap.builder()
                    .partitionLimits(Map.of(
                            partition(AFG_1, TASK_NAME_1), 10,
                            partition(AFG_2, TASK_NAME_2), 9
                        )
                    )
                    .taskNameNodeQuota(all(
                            fairQuotaDistribution(TASK_NAME_1, 1),
                            fairQuotaDistribution(TASK_NAME_2, 1, Set.of(NODES.get(8)))
                        )
                    )
                    .build(),
                Optional.of(NODES.get(9)),
                Optional.of(partition(AFG_1, TASK_NAME_1))
            )
        );
    }

    private static Stream<Arguments> shouldBatchRouteProvider() {
        return Stream.of(
            shouldRouteFair()
            , shouldRouteBaseOnCurrentLoadWhenNotEvenly()
            , shouldRouteBaseOnCurrentLoadWhenTaskEvenlyAndAllTasksUnevenly()
            , shouldRouteBaseOnEarliestAssignedWhenTaskEvenly()
            , shouldRouteBaseOnLastAffinityGroupAndTaskAssigned()
            , shouldRouteBaseOnClusterTaskLimit()
            , shouldRouteBaseOnNodeTaskLimit()
            , shouldCalculateCursorAndAffinityGroupTaskNameCorrectly()
        );
    }

    @ParameterizedTest
    @MethodSource("shouldBatchRouteProvider")
    void shouldBatchRoute(InputTestData inputTestData, ExpectedOutputData expectedOutputData) {
        //when
        TaskRouter taskRouter = new TaskRouter(
            inputTestData.nodeLastUpdateNumber(),
            inputTestData.partitionCursor()
        );

        //do
        BatchRouteMap batchRouteMap = taskRouter.batchRoute(inputTestData.batchRouteRequest());

        //verify
        assertThat(batchRouteMap).isEqualTo(expectedOutputData.batchRouteMap());
        expectedOutputData.affinityGroupTaskNameEntityCursor()
            .ifPresent(cursor -> Assertions.assertThat(taskRouter.affinityGroupTaskNameEntityCursor())
                .isPresent()
                .get()
                .isEqualTo(cursor)
            );
        expectedOutputData.lastUpdatedNode()
            .ifPresent(lastUpdatedNode -> assertThat(taskRouter.lastUpdatedNode())
                .isPresent()
                .get()
                .isEqualTo(lastUpdatedNode)
            );
    }


    private static Map<UUID, Long> updatedNumber(long... updatedNumber) {
        assertThat(NODES.size()).isEqualTo(updatedNumber.length);
        var result = Maps.<UUID, Long>newHashMap();
        IntStream.range(0, updatedNumber.length)
            .forEach(i -> result.put(NODES.get(i), updatedNumber[i]));
        return result;
    }

    private static Table<String, UUID, Integer> all(Table<String, UUID, Integer>... tables) {
        var builder = ImmutableTable.<String, UUID, Integer>builder();
        Arrays.stream(tables).forEach(builder::putAll);
        return builder.build();
    }

    private static Table<String, UUID, Integer> fairQuotaDistribution(String taskName, int quota) {
        return fairQuotaDistribution(taskName, quota, Collections.emptySet());
    }

    private static Table<String, UUID, Integer> fairQuotaDistribution(String taskName,
                                                                      int quota,
                                                                      Set<UUID> excludedNodes) {
        ImmutableTable.Builder<String, UUID, Integer> builder = ImmutableTable.builder();
        NODES.stream()
            .filter(nodeId -> !excludedNodes.contains(nodeId))
            .forEach(nodeId -> builder.put(taskName, nodeId, quota));
        return builder.build();
    }

    private static Table<String, UUID, Integer> singleQuota(String taskName, UUID node, int quota) {
        return ImmutableTable.of(taskName, node, quota);
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> all(List<T>... collections) {
        var builder = ImmutableList.<T>builder();
        Arrays.stream(collections).forEach(builder::addAll);
        return builder.build();
    }

    private static List<NodeTaskActivity> fairActivityDistribution(String taskName, int number) {
        return fairActivityDistribution(taskName, number, Collections.emptySet());
    }

    private static List<NodeTaskActivity> fairActivityDistribution(String taskName, int number, Set<UUID> excludedNodes) {
        var builder = ImmutableList.<NodeTaskActivity>builder();
        NODES.stream()
            .filter(nodeId -> !excludedNodes.contains(nodeId))
            .map(node -> NodeTaskActivity.builder()
                .node(node)
                .task(taskName)
                .number(number)
                .build()
            ).forEach(builder::add);
        return builder.build();
    }

    private static Partition partition(String afg, String taskName) {
        return Partition.builder()
            .affinityGroup(afg)
            .taskName(taskName)
            .build();
    }

    private static List<NodeCapacity> capacityForAllNodes(int total, List<NodeTaskActivity> nodeTaskActivities) {
        return NODES.stream()
            .map(node -> new NodeCapacity(node, SUPPORTED_TASKS, total))
            .peek(nodeCapacity -> nodeTaskActivities.stream()
                .filter(nodeActivity -> Objects.equals(nodeActivity.getNode(), nodeCapacity.getNode()))
                .forEach(nodeActivity -> nodeCapacity.busy(nodeActivity.getNumber()))
            )
            .toList();
    }

    private static List<NodeCapacity> capacityForAllNodes(int free) {
        return NODES.stream()
            .map(node -> capacityForAllTasks(node, free, 0))
            .toList();
    }

    private static NodeCapacity capacityForAllTasks(UUID node, int free, int busy) {
        NodeCapacity nodeCapacity = new NodeCapacity(node, SUPPORTED_TASKS, free);
        nodeCapacity.busy(busy);
        return nodeCapacity;
    }

    private static PartitionStat stat(String afk, String taskName, int number) {
        return PartitionStat.builder()
            .affinityGroup(afk)
            .taskName(taskName)
            .number(number)
            .build();
    }

    private record InputTestData(
        Map<UUID, Long> nodeLastUpdateNumber,
        Partition partitionCursor,
        BatchRouteRequest batchRouteRequest
    ) {
        public static InputTestData withoutState(BatchRouteRequest batchRouteRequest) {
            return new InputTestData(
                Maps.newHashMap(),
                null,
                batchRouteRequest
            );
        }

        public static InputTestData withNodeLastUpdateNumber(Map<UUID, Long> nodeLastUpdateNumber,
                                                             BatchRouteRequest batchRouteRequest) {
            return new InputTestData(
                nodeLastUpdateNumber,
                null,
                batchRouteRequest
            );
        }

        public static InputTestData withPartitionCursor(Partition cursor,
                                                        BatchRouteRequest batchRouteRequest) {
            return new InputTestData(
                Maps.newHashMap(),
                cursor,
                batchRouteRequest
            );
        }
    }

    private record ExpectedOutputData(
        BatchRouteMap batchRouteMap,
        Optional<UUID> lastUpdatedNode,
        Optional<Partition> affinityGroupTaskNameEntityCursor
    ) {
        public static ExpectedOutputData withoutInternalState(BatchRouteMap batchRouteMap) {
            return new ExpectedOutputData(
                batchRouteMap,
                Optional.empty(),
                Optional.empty()
            );
        }
    }
}