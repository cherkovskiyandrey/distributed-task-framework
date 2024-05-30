package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.AggregatedTaskStat;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.PlannedTask;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PlannerGroups;
import com.distributed_task_framework.service.internal.PlannerService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.CommonSettings;

import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class VirtualQueueStatHelper {
    CommonSettings commonSettings;
    TaskRegistryService taskRegistryService;
    TaskRepository taskRepository;
    TaskMapper taskMapper;
    MetricHelper metricHelper;
    MeterRegistry meterRegistry;
    AtomicReference<ImmutableList<AggregatedTaskStat>> aggregatedStatRef;
    ScheduledExecutorService watchdogExecutorService;
    String allTasksGaugeName;
    String notToPlanGaugeName;
    String movedCounterName;
    String plannedCounterName;
    List<Tag> commonManagerTags;
    List<Tag> commonPlannerTags;
    Timer aggregatedStatCalculationTimer;
    PlannerService plannerService;

    public VirtualQueueStatHelper(PlannerService plannerService,
                                  CommonSettings commonSettings,
                                  TaskRegistryService taskRegistryService,
                                  TaskRepository taskRepository,
                                  TaskMapper taskMapper,
                                  MetricHelper metricHelper,
                                  MeterRegistry meterRegistry) {
        this.plannerService = plannerService;
        this.commonSettings = commonSettings;
        this.taskRegistryService = taskRegistryService;
        this.taskRepository = taskRepository;
        this.taskMapper = taskMapper;
        this.metricHelper = metricHelper;
        this.meterRegistry = meterRegistry;
        this.aggregatedStatRef = new AtomicReference<>(ImmutableList.of());
        this.aggregatedStatCalculationTimer = metricHelper.timer("aggregatedStatCalculation", "time");
        this.allTasksGaugeName = metricHelper.buildName("planner", "task", "all");
        this.notToPlanGaugeName = metricHelper.buildName("planner", "task", "notToPlan");
        this.movedCounterName = metricHelper.buildName("planner", "task", "moved");
        this.plannedCounterName = metricHelper.buildName("planner", "task", "planned");
        this.commonManagerTags = List.of(Tag.of("group", PlannerGroups.VQB_MANAGER.getName()));
        this.commonPlannerTags = List.of(Tag.of("group", PlannerGroups.DEFAULT.getName()));
        this.watchdogExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("vq-stat")
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("virtualQueueStat(): error trying to calculate stat", e);
                            ReflectionUtils.rethrowRuntimeException(e);
                        })
                        .build()
        );
    }

    @PostConstruct
    public void init() {
        watchdogExecutorService.scheduleWithFixedDelay(
                ExecutorUtils.wrapRepeatableRunnable(this::calculateAggregatedStat),
                commonSettings.getStatSettings().getCalcInitialDelayMs(),
                commonSettings.getStatSettings().getCalcFixedDelayMs(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * @noinspection ResultOfMethodCallIgnored
     */
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): start of shutdown stat calculator");
        watchdogExecutorService.shutdownNow();
        watchdogExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): completed shutdown stat calculator");
    }

    @VisibleForTesting
    void calculateAggregatedStat() {
        //calculate only if my node is active planner in order to
        //take into account case when planner node is changed.
        //We want to have a valid sum of metrics among all nodes.
        if (!plannerService.isActive()) {
            return;
        }

        try {
            Map<UUID, Set<String>> registeredTaskByNode = taskRegistryService.getRegisteredLocalTaskInCluster();
            Set<String> knownTaskNames = registeredTaskByNode.values().stream()
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());

            List<AggregatedTaskStat> aggregatedTaskStat = Objects.requireNonNull(aggregatedStatCalculationTimer.record(
                            () -> taskRepository.getAggregatedTaskStat(knownTaskNames)
                    )
            );

            //update stat
            aggregatedStatRef.set(ImmutableList.copyOf(aggregatedTaskStat));

            //register newcomers
            aggregatedTaskStat.forEach(stat ->
                    Gauge.builder(
                                    allTasksGaugeName,
                                    () -> getOrCalculateAll(stat)
                            )
                            .tags(buildTags(stat))
                            .register(meterRegistry)
            );

            aggregatedTaskStat.stream()
                    .filter(stat -> !VirtualQueue.DELETED.equals(stat.getVirtualQueue()))
                    .forEach(stat ->
                            Gauge.builder(
                                            notToPlanGaugeName,
                                            () -> getOrCalculateNotToPlan(stat)
                                    )
                                    .tags(buildTags(stat))
                                    .register(meterRegistry)
                    );
        } catch (Exception e) {
            log.error("calculateAggregatedStat(): can't be calculated aggregated statistic", e);
        }
    }

    private Collection<Tag> buildTags(AggregatedTaskStat stat) {
        return ImmutableList.<Tag>builder()
                .addAll(commonManagerTags)
                .add(metricHelper.buildAffinityGroupTag(stat.getAffinityGroupName()))
                .add(metricHelper.buildVirtualQueueTag(stat.getVirtualQueue()))
                .add(Tag.of("task_name", stat.getTaskName()))
                .build();
    }

    private record AggregatedTaskStatKey(
            @Nullable
            String affinityGroupName,
            String taskName,
            @Nullable
            VirtualQueue virtualQueue
    ) {
    }

    private int getOrCalculateAll(AggregatedTaskStat aggregatedTaskStat) {
        var key = new AggregatedTaskStatKey(
                aggregatedTaskStat.getAffinityGroupName(),
                aggregatedTaskStat.getTaskName(),
                aggregatedTaskStat.getVirtualQueue()
        );
        return aggregatedStatRef.get()
                .stream()
                .collect(Collectors.groupingBy(
                        s -> new AggregatedTaskStatKey(s.getAffinityGroupName(), s.getTaskName(), s.getVirtualQueue()),
                        Collectors.summingInt(AggregatedTaskStat::getNumber)
                )).getOrDefault(key, 0);
    }

    private int getOrCalculateNotToPlan(AggregatedTaskStat aggregatedTaskStat) {
        var key = new AggregatedTaskStatKey(
                aggregatedTaskStat.getAffinityGroupName(),
                aggregatedTaskStat.getTaskName(),
                aggregatedTaskStat.getVirtualQueue()
        );
        return aggregatedStatRef.get()
                .stream()
                .filter(stat -> !VirtualQueue.DELETED.equals(stat.getVirtualQueue()))
                .filter(AggregatedTaskStat::isNotToPlanFlag)
                .collect(Collectors.groupingBy(
                        s -> new AggregatedTaskStatKey(s.getAffinityGroupName(), s.getTaskName(), s.getVirtualQueue()),
                        Collectors.summingInt(AggregatedTaskStat::getNumber)
                )).getOrDefault(key, 0);
    }

    public void updateMoved(Collection<ShortTaskEntity> movedShortTaskEntities) {
        var movedTasksStat = movedShortTaskEntities.stream()
                .collect(Collectors.groupingBy(
                        ShortTaskEntity::getVirtualQueue,
                        Collectors.groupingBy(
                                taskMapper::mapToPartition,
                                Collectors.counting()
                        )
                ));

        movedTasksStat.forEach((virtualQueue, movedTaskStat) ->
                {
                    for (var entry : movedTaskStat.entrySet()) {
                        Partition partition = entry.getKey();
                        List<Tag> tags = ImmutableList.<Tag>builder()
                                .addAll(commonManagerTags)
                                .add(metricHelper.buildVirtualQueueTag(virtualQueue))
                                .add(metricHelper.buildAffinityGroupTag(partition.getAffinityGroup()))
                                .add(Tag.of("task_name", partition.getTaskName()))
                                .build();
                        Counter.builder(movedCounterName)
                                .tags(tags)
                                .register(meterRegistry)
                                .increment(entry.getValue());
                    }
                }
        );
    }

    public void updatePlannedTasks(Collection<ShortTaskEntity> plannedTasks) {
        Map<PlannedTask, Long> plannedTaskStat = plannedTasks.stream()
                .collect(Collectors.groupingBy(
                        shortTaskEntity -> new PlannedTask(
                                shortTaskEntity.getAffinityGroup(),
                                shortTaskEntity.getTaskName(),
                                Objects.requireNonNull(shortTaskEntity.getAssignedWorker())
                        ),
                        Collectors.counting()
                ));

        for (var entry : plannedTaskStat.entrySet()) {
            final PlannedTask plannedTask = entry.getKey();
            List<Tag> tags = ImmutableList.<Tag>builder()
                    .addAll(commonPlannerTags)
                    .add(metricHelper.buildAffinityGroupTag(plannedTask.affinityGroup()))
                    .add(Tag.of("task_name", plannedTask.taskName()))
                    .add(Tag.of("worker_id", plannedTask.workerId().toString()))
                    .build();
            Counter.builder(plannedCounterName)
                    .tags(tags)
                    .register(meterRegistry)
                    .increment(entry.getValue());
        }
    }
}
