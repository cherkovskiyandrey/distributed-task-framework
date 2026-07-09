package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.IdVersionMapper;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.AffinityGroupAndAffinity;
import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.persistence.entity.IdVersionWithAffinityEntity;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.CapabilityRegister;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.DistributedTaskMetricHelper;
import com.distributed_task_framework.service.internal.PartitionTracker;
import com.distributed_task_framework.service.internal.PlannerGroups;
import com.distributed_task_framework.settings.CommonSettings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.PlatformTransactionManager;

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
    IdVersionMapper idVersionMapper;
    Set<AffinityGroupWrapper> affinityGroupsInNew = Sets.newHashSet();
    AtomicReference<LocalDateTime> lastScanAffinityGroupsDateTime = new AtomicReference<>(LocalDateTime.MIN);
    VirtualQueueStatService virtualQueueStatService;
    List<Tag> commonTags;
    Timer maxCreatedDateInNewTimer;
    Timer affinityGroupsInNewTimer;
    Timer affinityGroupInNewVirtualQueueTime;
    Timer taskFromNewTimer;
    Timer moveNewToReadyAndParkedTime;
    Timer readyToHardDeleteTime;
    Timer readyToMoveFromParkedToReadyTime;
    Timer moveParkedToReadyTime;
    Timer deleteByIdVersionTime;

    public VirtualQueueManagerPlannerImpl(CommonSettings commonSettings,
                                          PlannerRepository plannerRepository,
                                          PlatformTransactionManager transactionManager,
                                          ClusterProvider clusterProvider,
                                          TaskRepository taskRepository,
                                          PartitionTracker partitionTracker,
                                          TaskMapper taskMapper,
                                          IdVersionMapper idVersionMapper,
                                          VirtualQueueStatService virtualQueueStatService,
                                          DistributedTaskMetricHelper distributedTaskMetricHelper) {
        super(commonSettings, plannerRepository, transactionManager, clusterProvider, distributedTaskMetricHelper);
        this.plannerSettings = commonSettings.getPlannerSettings();
        this.clusterProvider = clusterProvider;
        this.taskRepository = taskRepository;
        this.partitionTracker = partitionTracker;
        this.taskMapper = taskMapper;
        this.idVersionMapper = idVersionMapper;
        this.virtualQueueStatService = virtualQueueStatService;
        this.commonTags = List.of(Tag.of("group", groupName()));
        this.maxCreatedDateInNewTimer = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "maxCreatedDateInNew", "time"),
            commonTags
        );
        this.affinityGroupsInNewTimer = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "afgInNewQueue", "time"),
            commonTags
        );
        this.affinityGroupInNewVirtualQueueTime = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "affinityGroupInNewVirtualQueue", "time"),
            commonTags
        );
        this.taskFromNewTimer = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "taskFromNew", "time"),
            commonTags
        );
        this.moveNewToReadyAndParkedTime = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "moveNewToReadyAndParked", "time"),
            commonTags
        );
        this.readyToHardDeleteTime = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "readyToHardDelete", "time"),
            commonTags
        );
        this.readyToMoveFromParkedToReadyTime = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "readyToMoveFromParkedToReady", "time"),
            commonTags
        );
        this.moveParkedToReadyTime = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "moveParkedToReadyBaseOnDeleted", "time"),
            commonTags
        );
        this.deleteByIdVersionTime = distributedTaskMetricHelper.timer(
            List.of("planner", "vqb", "deleteByIdVersion", "time"),
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
        if (!super.hasToBeActive()) {
            return false;
        }
        boolean doAllNodesSupportVQBPlanner = clusterProvider.doAllNodesSupport(Capabilities.VIRTUAL_QUEUE_MANAGER_PLANNER_V1);
        if (!doAllNodesSupportVQBPlanner) {
            log.warn("hasToBeActive(): doAllNodesSupportVQBPlanner = false");
            return false;
        }
        return true;
    }

    @Override
    protected void beforeStartLoop() {
        reset();
    }

    @Override
    protected void afterError() {
        reset();
    }

    //todo: возможен ли кейс что нода завершилась некорректно: успела переместить таски но не успела обновить partitionTracker
    // и при этом поднялся 2 планировщик на некоторое время в параллель и уже сделал partitionTracker.reinit(); до того как первый
    // перенес задачи и упал.
    private void reset() {
        partitionTracker.reinit();
        affinityGroupsInNew.clear();
        lastScanAffinityGroupsDateTime.set(LocalDateTime.MIN);
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
        var oldAffinityGroupsInNew = affinityGroupsInNew;
        var affinityGroupInNewStats = affinityGroupInNewVirtualQueueTime.record(() ->
            taskRepository.affinityGroupInNewVirtualQueueStat(
                affinityGroupsInNew,
                plannerSettings.getNewBatchSize()
            )
        );
        affinityGroupInNewStats = Objects.requireNonNull(affinityGroupInNewStats).stream()
            .filter(affinityGroupStat -> affinityGroupStat.getNumber() > 0)
            .collect(Collectors.toSet());
        updateAffinityGroupsAccordingToStat(affinityGroupInNewStats);


        if (affinityGroupInNewStats.isEmpty()) {
            log.debug("processNewQueue(): affinityGroupInNewStats is empty for affinityGroupsInNew=[{}]", oldAffinityGroupsInNew);
            return 0;
        }

        var affinityGroupsToMoveFromNew = calcAffinityGroupsToMoveFromNew(affinityGroupInNewStats);

        var idVersionWithAffinityFromNew = Objects.requireNonNull(taskFromNewTimer.recordCallable(
                () -> taskRepository.getTasksFromNew(affinityGroupsToMoveFromNew)
            )
        );

        if (idVersionWithAffinityFromNew.isEmpty()) {
            log.debug("processNewQueue(): idVersionWithAffinityFromNew is empty for affinityGroupsInNew=[{}]", oldAffinityGroupsInNew);
            return 0;
        }

        log.info("processNewQueue(): moveNewToReadyAndParked=[{}]", affinityGroupsToMoveFromNew);
        var movedTasks = Objects.requireNonNull(moveNewToReadyAndParkedTime.recordCallable(
                () -> taskRepository.moveNewToReadyAndParked(idVersionWithAffinityFromNew)
            )
        );
        if (movedTasks.isEmpty()) {
            log.debug("processNewQueue(): movedTasks is empty for affinityGroupsToMoveFromNew=[{}]", affinityGroupsToMoveFromNew);
            return 0;
        }

        virtualQueueStatService.updateMoved(movedTasks);
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
        var idVersionWithAffinityReadyToHardDelete = readyToHardDeleteTime.record(() ->
            taskRepository.readyToHardDelete(plannerSettings.getDeletedBatchSize())
        );
        if (Objects.requireNonNull(idVersionWithAffinityReadyToHardDelete).isEmpty()) {
            log.debug("processDeletedQueue(): idVersionWithAffinityEntity is empty");
            return 0;
        }

        var numMovedToReadyTasks = moveParkedToReady(idVersionWithAffinityReadyToHardDelete, activePartitions);

        log.info("processDeletedQueue(): idVersionWithAffinityReadyToHardDelete=[{}]", idVersionWithAffinityReadyToHardDelete);
        var idVersionToHardDelete = idVersionMapper.map(idVersionWithAffinityReadyToHardDelete);
        var deletedTaskIdVersions = Objects.requireNonNull(deleteByIdVersionTime.recordCallable(
            () -> taskRepository.deleteByIdVersion(idVersionToHardDelete))
        );
        var conflictedTaskIdVersions = Sets.difference(idVersionWithAffinityReadyToHardDelete, Sets.newHashSet(deletedTaskIdVersions));
        if (!conflictedTaskIdVersions.isEmpty()) {
            log.error("processDeletedQueue(): CONFLICTED conflictedTaskIdVersions=[{}]", conflictedTaskIdVersions);
        }

        return numMovedToReadyTasks;
    }

    private int moveParkedToReady(Set<IdVersionWithAffinityEntity> readyToHardDelete, Set<Partition> activePartitions) throws Exception {
        var affinityGroupWithAffinity = readyToHardDelete.stream()
            .filter(entity -> StringUtils.isNotBlank(entity.getAffinity()))
            .map(entity -> new AffinityGroupAndAffinity(entity.getAffinityGroup(), entity.getAffinity()))
            .collect(Collectors.toSet());
        if (affinityGroupWithAffinity.isEmpty()) {
            log.debug("moveParkedToReady(): affinityGroupWithAffinity is empty");
            return 0;
        }

        var parkedToReady = Objects.requireNonNull(readyToMoveFromParkedToReadyTime.recordCallable(
                () -> taskRepository.readyToMoveFromParkedToReady(affinityGroupWithAffinity)
            )
        );
        if (parkedToReady.isEmpty()) {
            log.debug("moveParkedToReady(): parkedToReady is empty");
            return 0;
        }

        var movedToReadyTasks = Objects.requireNonNull(moveParkedToReadyTime.recordCallable(
                () -> taskRepository.moveParkedToReady(parkedToReady)
            )
        );
        if (movedToReadyTasks.isEmpty()) {
            log.warn("moveParkedToReady(): movedToReadyTasks is empty, suspect to concurrent planner is run!!!");
            return 0;
        }
        if (!Objects.equals(parkedToReady.size(), movedToReadyTasks.size())) {
            log.warn(
                "moveParkedToReady(): parkedToReady.size=[{}] != movedToReadyTasks.size=[{}], suspect to concurrent planner is run!!!",
                parkedToReady.size(),
                movedToReadyTasks.size()
            );
        }

        virtualQueueStatService.updateMoved(movedToReadyTasks);
        log.info("moveParkedToReady(): movedTasksDescription=[{}]", movedTasksToDescription(movedToReadyTasks));

        var partitionsFromParked = movedToReadyTasks.stream()
            .map(taskMapper::mapToPartition)
            .collect(Collectors.toSet());
        log.info("moveParkedToReady(): partitionsFromParked=[{}]", partitionsFromParked);
        activePartitions.addAll(partitionsFromParked);

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
                var affinityGroupName = source.getAffinityGroup();
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
        LocalDateTime prevMaxDateTime = lastScanAffinityGroupsDateTime.get();
        LocalDateTime maxDateTime = maxCreatedDateInNewTimer.record(() ->
            taskRepository.maxCreatedDateInNewVirtualQueue().orElse(EPOCH)
        );
        Set<AffinityGroupWrapper> detectedAffinityGroups = affinityGroupsInNewTimer.record(() ->
            taskRepository.affinityGroupsInNewVirtualQueue(
                prevMaxDateTime,
                plannerSettings.getAffinityGroupScannerTimeOverlap()
            )
        );
        affinityGroupsInNew.addAll(Objects.requireNonNull(detectedAffinityGroups));
        lastScanAffinityGroupsDateTime.set(maxDateTime);
        log.debug(
            "updateAffinityGroupsInNewVirtualQueue(): detectedAffinityGroups=[{}], affinityGroupsInNew=[{}]",
            detectedAffinityGroups,
            affinityGroupsInNew
        );
    }

    private void updateAffinityGroupsAccordingToStat(Set<AffinityGroupStat> affinityGroupInNewStats) {
        affinityGroupsInNew.clear();
        affinityGroupsInNew.addAll(affinityGroupInNewStats.stream()
            .map(affinityGroupStat -> AffinityGroupWrapper.builder()
                .affinityGroup(affinityGroupStat.getAffinityGroup())
                .build()
            )
            .collect(Collectors.toSet())
        );
    }

    private String movedTasksToDescription(List<ShortTaskEntity> movedTasks) {
        return movedTasks.stream()
            .map(shortTaskEntity -> shortTaskEntity.getId() + " => " + shortTaskEntity.getVirtualQueue())
            .collect(Collectors.joining(", "));
    }
}
