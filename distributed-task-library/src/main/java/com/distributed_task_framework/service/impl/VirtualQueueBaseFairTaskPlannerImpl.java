package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.BatchRouteMap;
import com.distributed_task_framework.model.BatchRouteRequest;
import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.NodeCapacity;
import com.distributed_task_framework.model.NodeLoading;
import com.distributed_task_framework.model.NodeTaskActivity;
import com.distributed_task_framework.model.PartitionStat;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.CapabilityRegister;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PlannerGroups;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.distributed_task_framework.settings.CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS;


@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class VirtualQueueBaseFairTaskPlannerImpl extends AbstractPlannerImpl implements CapabilityRegister {
    public static final String PLANNER_NAME = "virtual queue base planner";
    public static final String PLANNER_SHORT_NAME = "vqbp";

    ClusterProvider clusterProvider;
    TaskRepository taskRepository;
    TaskRegistryService taskRegistryService;
    TaskRouter taskRouter;
    PartitionTracker partitionTracker;
    Integer maxParallelTasksInClusterDefault;
    VirtualQueueStatHelper virtualQueueStatHelper;
    Clock clock;
    List<Tag> commonTags;
    Timer currentAssignedTaskStatTimer;
    Timer partitionsFromNewStatTimer;
    Timer batchRouteTimer;
    Timer loadTasksToPlanTimer;

    public VirtualQueueBaseFairTaskPlannerImpl(CommonSettings commonSettings,
                                               PlannerRepository plannerRepository,
                                               PlatformTransactionManager transactionManager,
                                               ClusterProvider clusterProvider,
                                               TaskRepository taskRepository,
                                               PartitionTracker partitionTracker,
                                               TaskRegistryService taskRegistryService,
                                               TaskRouter taskRouter,
                                               VirtualQueueStatHelper virtualQueueStatHelper,
                                               Clock clock,
                                               MetricHelper metricHelper) {
        super(commonSettings, plannerRepository, transactionManager, clusterProvider, metricHelper);
        this.clusterProvider = clusterProvider;
        this.taskRepository = taskRepository;
        this.partitionTracker = partitionTracker;
        this.taskRegistryService = taskRegistryService;
        this.taskRouter = taskRouter;
        this.virtualQueueStatHelper = virtualQueueStatHelper;
        this.maxParallelTasksInClusterDefault = commonSettings.getPlannerSettings().getMaxParallelTasksInClusterDefault();
        this.clock = clock;
        this.commonTags = List.of(Tag.of("group", groupName()));
        this.currentAssignedTaskStatTimer = metricHelper.timer(
                List.of("planner", "vqb", "currentAssignedTaskStat", "time"),
                commonTags
        );
        this.partitionsFromNewStatTimer = metricHelper.timer(
                List.of("planner", "vqb", "partitionsFromNewStatTimer", "time"),
                commonTags
        );
        this.batchRouteTimer = metricHelper.timer(
                List.of("planner", "vqb", "batchRouteTimer", "time"),
                commonTags
        );
        this.loadTasksToPlanTimer = metricHelper.timer(
                List.of("planner", "vqb", "loadTasksToPlanTimer", "time"),
                commonTags
        );
    }

    @Override
    protected String name() {
        return PLANNER_NAME;
    }

    @Override
    protected String shortName() {
        return PLANNER_SHORT_NAME;
    }

    @Override
    protected String groupName() {
        return PlannerGroups.DEFAULT.getName();
    }

    @Override
    public EnumSet<Capabilities> capabilities() {
        return EnumSet.of(Capabilities.VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_V1);
    }

    @Override
    protected boolean hasToBeActive() {
        boolean doAllNodesSupportVQBPlanner = clusterProvider.doAllNodesSupport(Capabilities.VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_V1);
        if (!doAllNodesSupportVQBPlanner) {
            log.debug("hasToBeActive(): doAllNodesSupportVQBPlanner = false");
            return false;
        }
        return true;
    }

    @Override
    @SneakyThrows
    int processInLoop() {
        Set<UUID> availableNodes = availableNodesCalculation();
        if (availableNodes.isEmpty()) {
            log.warn("processInLoop(): there aren't available nodes to plan or all nodes overloaded");
            return 0;
        }

        Map<UUID, Set<String>> registeredTaskByNode = taskRegistryService.getRegisteredLocalTaskInCluster();
        Set<UUID> knownNodes = registeredTaskByNode.keySet();
        Set<String> knownTaskNames = registeredTaskByNode.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toSet());

        Map<UUID, Set<String>> availableTaskByNode = registeredTaskByNode.entrySet().stream()
                .filter(entry -> availableNodes.contains(entry.getKey()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue

                ));
        Set<String> availableTaskNames = availableTaskByNode.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toSet());
        if (availableTaskNames.isEmpty()) {
            log.debug("processInLoop(): there aren't registered tasks on available nodes to plan");
            return 0;
        }

        var activeTaskNameAndAffinityGroups = partitionTracker.getAll().stream()
                .filter(entity -> availableTaskNames.contains(entity.getTaskName()))
                .collect(Collectors.toSet());
        if (activeTaskNameAndAffinityGroups.isEmpty()) {
            log.debug("processInLoop(): activeTaskNameAndAffinityGroups are empty");
            return 0;
        }

        List<NodeTaskActivity> nodeTaskActivities = Objects.requireNonNull(
                currentAssignedTaskStatTimer.record(
                        () -> taskRepository.currentAssignedTaskStat(knownNodes, knownTaskNames)
                )
        );
        List<NodeCapacity> nodeCapacities = calculateNodeCapacities(
                availableTaskByNode,
                nodeTaskActivities
        );
        if (nodeCapacities.isEmpty()) {
            log.debug("processInLoop(): nodeCapacities is empty");
            return 0;
        }

        int clusterCapacity = nodeCapacities.stream()
                .mapToInt(NodeCapacity::getFreeCapacity)
                .sum();
        Set<PartitionStat> partitionStats = Objects.requireNonNull(
                partitionsFromNewStatTimer.record(() ->
                        taskRepository.findPartitionStatToPlan(
                                knownNodes,
                                activeTaskNameAndAffinityGroups,
                                clusterCapacity
                        )
                )
        );

        Set<String> taskNamesToPlan = partitionStats.stream()
                .map(PartitionStat::getTaskName)
                .collect(Collectors.toSet());
        Map<String, Integer> currentActiveTasksByName = nodeTaskActivities.stream()
                .collect(Collectors.groupingBy(
                        NodeTaskActivity::getTask,
                        Collectors.summingInt(NodeTaskActivity::getNumber)
                ));

        Map<String, Integer> actualTaskLimits = applyLimits(taskNamesToPlan, currentActiveTasksByName);
        boolean isReachedLimits = actualTaskLimits.values().stream()
                .allMatch(limit -> limit == 0);
        if (isReachedLimits) {
            log.debug("processInLoop(): isReachedLimits=true");
            return 0;
        }

        BatchRouteMap batchRouteMap = Objects.requireNonNull(
                batchRouteTimer.record(() -> taskRouter.batchRoute(BatchRouteRequest.builder()
                                .newTaskBatches(partitionStats)
                                .actualTaskLimits(actualTaskLimits)
                                .nodeTaskActivities(nodeTaskActivities)
                                .nodeCapacities(nodeCapacities)
                                .build()
                        )
                )
        );
        if (batchRouteMap.getPartitionLimits().isEmpty()) {
            log.debug("processInLoop(): partitionLimits is empty");
            return 0;
        }

        Collection<ShortTaskEntity> unplannedActualTasks = Objects.requireNonNull(
                loadTasksToPlanTimer.record(() -> taskRepository.loadTasksToPlan(
                                knownNodes,
                                batchRouteMap.getPartitionLimits()
                        )
                )
        );
        if (unplannedActualTasks.isEmpty()) {
            log.debug("planTaskFromActiveQueue(): there isn't unplanned tasks");
            return 0;
        }

        log.info("planTaskFromActiveQueue(): batchRouteMap=[{}]", batchRouteMap);
        Collection<ShortTaskEntity> plannedTasks = assignNodeToTasks(
                unplannedActualTasks,
                batchRouteMap.getTaskNameNodeQuota()
        );
        plannedTasks = sort(plannedTasks); //to prevent deadlocks during split brain
        taskRepository.updateAll(plannedTasks);
        virtualQueueStatHelper.updatePlannedTasks(plannedTasks);
        log.info(
                "planTaskFromActiveQueue(): unplannedActualTasks=[{}], plannedTasks=[{}]",
                toIdList(unplannedActualTasks),
                toIdList(plannedTasks)
        );

        return plannedTasks.size();
    }

    private Set<UUID> availableNodesCalculation() {
        return clusterProvider.currentNodeLoading().stream()
                .filter(nodeLoading -> {
                    int compareResult = Double.compare(
                            nodeLoading.getMedianCpuLoading(),
                            commonSettings.getPlannerSettings().getNodeCpuLoadingLimit()
                    );
                    return compareResult < 0;
                })
                .map(NodeLoading::getNode)
                .collect(Collectors.toSet());
    }

    private Collection<ShortTaskEntity> assignNodeToTasks(Collection<ShortTaskEntity> unplannedActualTasks,
                                                          Table<String, UUID, Integer> taskNameNodeQuota) {
        return unplannedActualTasks.stream()
                .map(shortTaskEntity -> {
                    String taskName = shortTaskEntity.getTaskName();
                    Map<UUID, Integer> nodeToQuotaMap = taskNameNodeQuota.row(taskName);
                    if (nodeToQuotaMap.isEmpty()) {
                        log.error(
                                "assignNodeToTasks(): couldn't find quota for shortTaskEntity=[{}] in taskNameNodeQuota=[{}]",
                                shortTaskEntity,
                                taskNameNodeQuota
                        );
                        return shortTaskEntity;
                    }
                    Map.Entry<UUID, Integer> firstNodeToQuota = nodeToQuotaMap.entrySet().iterator().next();
                    UUID nodeId = firstNodeToQuota.getKey();
                    int newQuota = firstNodeToQuota.getValue() - 1;
                    if (newQuota == 0) {
                        taskNameNodeQuota.remove(taskName, nodeId);
                    } else {
                        taskNameNodeQuota.put(taskName, nodeId, newQuota);
                    }
                    return shortTaskEntity.toBuilder()
                            .assignedWorker(nodeId)
                            .lastAssignedDateUtc(LocalDateTime.now(clock))
                            .build();
                })
                .filter(shortTaskEntity -> shortTaskEntity.getAssignedWorker() != null)
                .toList();
    }

    private List<UUID> toIdList(Collection<ShortTaskEntity> plannedTasks) {
        return plannedTasks.stream()
                .map(ShortTaskEntity::getId)
                .toList();
    }

    private Collection<ShortTaskEntity> sort(Collection<ShortTaskEntity> plannedTasks) {
        return plannedTasks.stream()
                .sorted(ShortTaskEntity.COMPARATOR)
                .toList();
    }

    private Map<String, Integer> applyLimits(Set<String> potentialTasksToAssign,
                                             Map<String, Integer> currentActiveTasksByName) {
        Map<String, Integer> limits = Maps.newHashMap();
        for (String taskName : potentialTasksToAssign) {
            int allowedTaskNumber = calculateAllowedTaskNumber(taskName, currentActiveTasksByName);
            if (allowedTaskNumber == 0) {
                log.debug("applyLimits(): capacity is exhausted for task=[{}], currentTasks in cluster=[{}]",
                        taskName, currentActiveTasksByName.getOrDefault(taskName, 0));
            }
            limits.put(taskName, allowedTaskNumber);
        }
        return limits;
    }

    private int calculateAllowedTaskNumber(String taskName, Map<String, Integer> currentActiveTasksByName) {
        int maxParallelInCluster = taskRegistryService.getLocalTaskParameters(taskName)
                .map(TaskSettings::getMaxParallelInCluster)
                .orElseGet(() -> {
                    log.warn("calculateAllowedTaskNumber(): unknown parameters for task=[{}], use default limit", taskName);
                    return maxParallelTasksInClusterDefault;
                });
        if (maxParallelInCluster == UNLIMITED_PARALLEL_TASKS) {
            return UNLIMITED_PARALLEL_TASKS;
        }
        int currentActiveTasks = currentActiveTasksByName.getOrDefault(taskName, 0);
        return Math.max(maxParallelInCluster - currentActiveTasks, 0);
    }

    private List<NodeCapacity> calculateNodeCapacities(Map<UUID, Set<String>> availableTaskByNode,
                                                       List<NodeTaskActivity> nodeTaskActivities) {
        List<NodeCapacity> result = Lists.newArrayList();
        int maxParallelTasksInNode = commonSettings.getWorkerManagerSettings().getMaxParallelTasksInNode();
        for (var entry : availableTaskByNode.entrySet()) {
            UUID node = entry.getKey();
            Set<String> supportedTasks = entry.getValue();
            NodeCapacity nodeCapacity = new NodeCapacity(node, supportedTasks, maxParallelTasksInNode);
            result.add(nodeCapacity);
            for (var nodeTaskActivity : nodeTaskActivities) {
                int busyTaskNumber = nodeTaskActivity.getNumber();
                if (node.equals(nodeTaskActivity.getNode())) {
                    nodeCapacity.busy(busyTaskNumber);
                }
            }
        }

        Float planFactor = commonSettings.getPlannerSettings().getPlanFactor();
        return result.stream()
                .filter(nodeCapacity -> nodeCapacity.getFreeCapacity() > 0)
                .peek(nodeCapacity -> nodeCapacity.setFreeCapacity((int) (nodeCapacity.getFreeCapacity() * planFactor)))
                .toList();
    }
}
