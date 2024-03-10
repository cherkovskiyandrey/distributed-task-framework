package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.Partition;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.CapabilityRegister;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.PlannerGroups;
import com.distributed_task_framework.settings.CommonSettings;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class VirtualQueueManagerPlannerImpl extends AbstractPlannerImpl implements CapabilityRegister {
    public static final String PLANNER_NAME = "virtual queue manager";
    public static final String PLANNER_SHORT_NAME = "vqm";

    private static final LocalDateTime EPOCH = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

    CommonSettings.PlannerSettings plannerSettings;
    ClusterProvider clusterProvider;
    TaskRepository taskRepository;
    PartitionTracker partitionTracker;
    TaskMapper taskMapper;
    Set<AffinityGroupWrapper> affinityGroupsInNew = Sets.newHashSet();
    AtomicReference<LocalDateTime> lastScanAffinityGroupsDateTime = new AtomicReference<>(LocalDateTime.MIN);
    VirtualQueueStatHelper virtualQueueStatHelper;
    List<Tag> commonTags;
    Timer affinityGroupsInNewTimer;
    Timer moveNewToReadyTime;
    Timer moveParkedToReadyTime;

    public VirtualQueueManagerPlannerImpl(CommonSettings commonSettings,
                                          PlannerRepository plannerRepository,
                                          PlatformTransactionManager transactionManager,
                                          ClusterProvider clusterProvider,
                                          TaskRepository taskRepository,
                                          PartitionTracker partitionTracker,
                                          TaskMapper taskMapper,
                                          VirtualQueueStatHelper virtualQueueStatHelper,
                                          MetricHelper metricHelper) {
        super(commonSettings, plannerRepository, transactionManager, clusterProvider, metricHelper);
        this.plannerSettings = commonSettings.getPlannerSettings();
        this.clusterProvider = clusterProvider;
        this.taskRepository = taskRepository;
        this.partitionTracker = partitionTracker;
        this.taskMapper = taskMapper;
        this.virtualQueueStatHelper = virtualQueueStatHelper;
        this.commonTags = List.of(Tag.of("group", groupName()));
        this.affinityGroupsInNewTimer = metricHelper.timer(
                List.of("planner", "vqb", "afgInNewQueue", "time"),
                commonTags
        );
        this.moveNewToReadyTime = metricHelper.timer(
                List.of("planner", "vqb", "moveNewToReady", "time"),
                commonTags
        );
        this.moveParkedToReadyTime = metricHelper.timer(
                List.of("planner", "vqb", "moveParkedToReadyBaseOnDeleted", "time"),
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
        return PlannerGroups.VQB_MANAGER.getName();
    }

    @Override
    protected boolean inTransaction() {
        return false;
    }

    @Override
    public EnumSet<Capabilities> capabilities() {
        return EnumSet.of(Capabilities.VIRTUAL_QUEUE_MANAGER_PLANNER_V1);
    }

    @Override
    protected boolean hasToBeActive() {
        boolean doAllNodesSupportVQBPlanner = clusterProvider.doAllNodesSupport(Capabilities.VIRTUAL_QUEUE_MANAGER_PLANNER_V1);
        if (!doAllNodesSupportVQBPlanner) {
            log.debug("hasToBeActive(): doAllNodesSupportVQBPlanner = false");
            return false;
        }
        return true;
    }

    @Override
    protected void beforeStartLoop() {
        partitionTracker.reinit();
        affinityGroupsInNew.clear();
    }

    @Override
    @SneakyThrows
    int processInLoop() {
        updateAffinityGroupsInNewVirtualQueue();

        Set<Partition> activePartitions = Sets.newHashSet();
        int movedTasks = processNewQueue(activePartitions);
        movedTasks += processDeletedQueue(activePartitions);

        partitionTracker.track(activePartitions);
        partitionTracker.compactIfNecessary();
        partitionTracker.gcIfNecessary();

        return movedTasks;
    }

    private int processNewQueue(Set<Partition> activePartitions) throws Exception {
        var affinityGroupInNewStats = taskRepository.affinityGroupInNewVirtualQueueStat(
                affinityGroupsInNew,
                plannerSettings.getNewBatchSize()
        );

        if (affinityGroupInNewStats.isEmpty()) {
            return 0;
        }

        var affinityGroupsToMoveFromNew = calcAffinityGroupsToMoveFromNew(affinityGroupInNewStats);
        var movedTasks = Objects.requireNonNull(moveNewToReadyTime.recordCallable(
                        () -> taskRepository.moveNewToReady(affinityGroupsToMoveFromNew)
                )
        );
        if (movedTasks.isEmpty()) {
            return 0;
        }

        virtualQueueStatHelper.updateMoved(movedTasks);
        log.info("processNewQueue(): movedTasksDescription=[{}]", movedTasksToDescription(movedTasks));

        var partitionsFromNew = movedTasks.stream()
                .filter(shortTaskEntity -> VirtualQueue.READY.equals(shortTaskEntity.getVirtualQueue()))
                .map(taskMapper::mapToPartition)
                .collect(Collectors.toSet());
        if (!partitionsFromNew.isEmpty()) {
            log.debug("processNewQueue(): partitionsFromNew=[{}]", partitionsFromNew);
            activePartitions.addAll(partitionsFromNew);
        }

        return movedTasks.size();
    }

    private int processDeletedQueue(Set<Partition> activePartitions) throws Exception {
        var taskIdsToDelete = taskRepository.readyToHardDelete(plannerSettings.getDeletedBatchSize());
        if (taskIdsToDelete.isEmpty()) {
            return 0;
        }

        var movedToReadyTasks = Objects.requireNonNull(moveParkedToReadyTime.recordCallable(
                        () -> taskRepository.moveParkedToReady(taskIdsToDelete.size())
                )
        );

        if (!movedToReadyTasks.isEmpty()) {
            virtualQueueStatHelper.updateMoved(movedToReadyTasks);
            log.info("processDeletedQueue(): movedTasksDescription=[{}]", movedTasksToDescription(movedToReadyTasks));

            var partitionsFromParked = movedToReadyTasks.stream()
                    .map(taskMapper::mapToPartition)
                    .collect(Collectors.toSet());
            log.info("processDeletedQueue(): partitionsFromParked=[{}]", partitionsFromParked);
            activePartitions.addAll(partitionsFromParked);
        }

        log.info("processDeletedQueue(): taskIdsToDelete=[{}]", taskIdsToDelete);
        taskRepository.deleteByIds(taskIdsToDelete);
        return movedToReadyTasks.size();
    }

    private Set<AffinityGroupStat> calcAffinityGroupsToMoveFromNew(Set<AffinityGroupStat> affinityGroupStats) {
        affinityGroupStats = Sets.newHashSet(affinityGroupStats);
        int capacity = plannerSettings.getNewBatchSize();
        Map<String, AffinityGroupStat> affinityGroupStatsByAffinityGroup = Maps.newHashMap();
        while (capacity > 0 && !affinityGroupStats.isEmpty()) {
            Set<AffinityGroupStat> toRemove = Sets.newHashSet();
            for (var source : affinityGroupStats) {
                if (capacity == 0) {
                    break;
                }
                if (source.getNumber() == 0) {
                    toRemove.add(source);
                    continue;
                }
                var affinityGroupName = source.getAffinityGroupName();
                var sink = affinityGroupStatsByAffinityGroup.computeIfAbsent(
                        affinityGroupName,
                        key -> new AffinityGroupStat(key, 0)
                );
                capacity--;
                source.decreaseNumber();
                sink.increaseNumber();
            }
            affinityGroupStats.removeAll(toRemove);
        }
        return Sets.newHashSet(affinityGroupStatsByAffinityGroup.values());
    }

    private void updateAffinityGroupsInNewVirtualQueue() {
        LocalDateTime from = lastScanAffinityGroupsDateTime.getAndSet(
                taskRepository.maxCreatedDateInNewVirtualQueue().orElse(EPOCH)
        );
        affinityGroupsInNewTimer.record(() ->
                affinityGroupsInNew.addAll(
                        taskRepository.affinityGroupsInNewVirtualQueue(
                                from,
                                plannerSettings.getAffinityGroupScannerTimeOverlap()
                        )
                )
        );
        log.debug("updateAffinityGroupsInNewVirtualQueue(): known affinityGroups=[{}]", affinityGroupsInNew);
    }

    private String movedTasksToDescription(List<ShortTaskEntity> movedTasks) {
        return movedTasks.stream()
                .map(shortTaskEntity -> shortTaskEntity.getId() + " => " + shortTaskEntity.getVirtualQueue())
                .collect(Collectors.joining(", "));
    }
}
