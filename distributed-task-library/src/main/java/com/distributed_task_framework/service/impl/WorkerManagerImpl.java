package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.TaskWorkerFactory;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class WorkerManagerImpl implements WorkerManager {
    private record ActiveTask(
            TaskEntity taskEntity,
            Future<Void> future,
            AtomicBoolean isCompleted) {
    }

    CommonSettings commonSettings;
    CommonSettings.WorkerManagerSettings workerManagerSettings;
    TaskRegistryService taskRegistryService;
    ClusterProvider clusterProvider;
    TaskRepository taskRepository;
    TaskWorkerFactory taskWorkerFactory;
    Map<TaskId, ActiveTask> activeTasks;
    Multimap<Instant, TaskId> expiredDateToTask;
    Set<TaskId> interruptedTasks;
    ExecutorService workerManagerExecutorService;
    ExecutorService workersExecutorService;
    TaskMapper taskMapper;
    Clock clock;
    MetricHelper metricHelper;

    public WorkerManagerImpl(CommonSettings commonSettings,
                             ClusterProvider clusterProvider,
                             TaskRegistryService taskRegistryService,
                             TaskWorkerFactory taskWorkerFactory,
                             TaskRepository taskRepository,
                             TaskMapper taskMapper,
                             Clock clock,
                             MetricHelper metricHelper) {
        this.commonSettings = commonSettings;
        this.clusterProvider = clusterProvider;
        this.workerManagerSettings = commonSettings.getWorkerManagerSettings();
        this.taskRegistryService = taskRegistryService;
        this.taskWorkerFactory = taskWorkerFactory;
        this.taskRepository = taskRepository;
        this.taskMapper = taskMapper;
        this.clock = clock;
        this.activeTasks = Maps.newHashMap();
        this.expiredDateToTask = MultimapBuilder.treeKeys().arrayListValues().build();
        this.interruptedTasks = Sets.newHashSet();
        this.metricHelper = metricHelper;
        this.workerManagerExecutorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("worker-mng-%d")
                .setUncaughtExceptionHandler((t, e) -> {
                    log.error("worker-manager(): unexpected error", e);
                    ReflectionUtils.rethrowRuntimeException(e);
                })
                .build()
        );
        this.workersExecutorService = new ThreadPoolExecutor(0, this.workerManagerSettings.getMaxParallelTasksInNode(),
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("worker-%d")
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("worker(): unexpected error", e);
                            ReflectionUtils.rethrowRuntimeException(e);
                        })
                        .build()
        );
    }

    @PostConstruct
    public void init() {
        workerManagerExecutorService.submit(ExecutorUtils.wrapRepeatableRunnable(this::manageLoop));
    }

    /**
     * @noinspection ResultOfMethodCallIgnored
     */
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): shutdown started");
        workerManagerExecutorService.shutdownNow();
        workersExecutorService.shutdown();
        workerManagerExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        workersExecutorService.shutdownNow();
        workersExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): shutdown completed");
    }

    @VisibleForTesting
    void manageLoop() {
        log.info("manageLoop(): has been started");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                @SuppressWarnings("DataFlowIssue")
                int taskNumber = getManageTimer().record(this::manageTasks);
                getManagedTasks().increment(taskNumber);
                log.trace("manageLoop(): {}", taskNumber);
                try {
                    sleep(taskNumber);
                } catch (InterruptedException e) {
                    log.info("manageLoop(): has been interrupted.");
                    return;
                }
            } catch (Exception exception) {
                log.error("manageLoop(): manage error!", exception);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    log.info("manageLoop(): has been interrupted.");
                    return;
                }
            }
        }
        log.info("manageLoop(): has been stopped");
    }

    @VisibleForTesting
    int manageTasks() {
        cleanExpiredTasks();
        cleanFinishedTasks();
        return startNewTasks();
    }

    /**
     * @noinspection unchecked
     */
    private int startNewTasks() {
        int currentActiveTasks = activeTasks.size();
        int freeCapacity = Math.max(workerManagerSettings.getMaxParallelTasksInNode() - currentActiveTasks, 0);
        if (freeCapacity == 0) {
            log.info("manageTasks(): there isn't free workers");
            return 0;
        }

        Collection<TaskEntity> newTasks = taskRepository.getNextTasks(
                clusterProvider.nodeId(),
                activeTasks.keySet(),
                freeCapacity
        );
        var starvation = freeCapacity - newTasks.size();
        log.debug("manageTasks(): starvation = {}", starvation);
        if (starvation > 0) {
            getStarvationTasks().increment(starvation);
        }

        List<TaskEntity> unknownTasks = Lists.newArrayList();
        for (TaskEntity taskEntity : newTasks) {
            log.info("startNewTasks(): task=[{}] has been started, details=[{}]", taskEntity.getId(), taskEntity);
            TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
            Optional<RegisteredTask<Object>> registeredTaskOpt = taskRegistryService.getRegisteredLocalTask(
                    taskEntity.getTaskName()
            );
            if (registeredTaskOpt.isEmpty()) {
                log.warn("startNewTasks(): planned task=[{}] has been unregistered", taskId);
                unknownTasks.add(taskEntity);
                getUnknownTaskCounter(taskEntity).increment();
            } else {
                RegisteredTask<Object> registeredTask = registeredTaskOpt.get();
                TaskSettings taskSettings = registeredTask.getTaskSettings();
                TaskWorker taskWorker = taskWorkerFactory.buildTaskWorker(
                        taskEntity,
                        taskSettings
                );
                AtomicBoolean isCompleted = new AtomicBoolean(false);
                Future<Void> taskWorkerFuture = (Future<Void>) workersExecutorService.submit(() -> {
                            try {
                                taskWorker.execute(taskEntity, registeredTask);
                            } finally {
                                isCompleted.set(true);
                            }
                        }
                );
                activeTasks.putIfAbsent(taskId, new ActiveTask(taskEntity, taskWorkerFuture, isCompleted));
                registerToTrackExpirationIfRequired(taskId, taskSettings);
            }
        }
        revertUnknownTasks(unknownTasks);

        return newTasks.size() - unknownTasks.size();
    }

    private void registerToTrackExpirationIfRequired(TaskId taskId, TaskSettings taskSettings) {
        if (taskSettings.getTimeout() != null
                && Duration.ZERO.compareTo(taskSettings.getTimeout()) < 0) {
            Instant expiredDate = Instant.now(clock).plus(taskSettings.getTimeout());
            expiredDateToTask.put(expiredDate, taskId);
            log.info("registerToTrackExpirationIfRequired(): taskId=[{}] will be expired at [{}]", taskId.getId(), expiredDate);
        }
    }

    private void revertUnknownTasks(List<TaskEntity> unknownTasks) {
        var unknownShortTasks = unknownTasks.stream()
                .map(taskEntity -> taskEntity.toBuilder()
                        .assignedWorker(null)
                        .lastAssignedDateUtc(null)
                        .build())
                .map(taskMapper::mapToShort)
                .collect(Collectors.toList());
        taskRepository.updateAll(unknownShortTasks);
    }

    private void cleanExpiredTasks() {
        Instant now = Instant.now(clock);
        SortedSet<Instant> instants = (SortedSet<Instant>) expiredDateToTask.keySet();
        Set<Instant> expiredInstants = Sets.newHashSet(instants.headSet(now));

        Set<TaskId> expiredTaskIds = expiredInstants.stream()
                .flatMap(instant -> expiredDateToTask.get(instant).stream())
                .filter(taskId -> !interruptedTasks.contains(taskId))
                .collect(Collectors.toSet());
        Set<TaskId> notExpiredTaskIds = Sets.newHashSet(Sets.difference(activeTasks.keySet(), expiredTaskIds));
        Set<TaskId> canceledTaskIds = Sets.newHashSet(Sets.difference(
                        taskRepository.filterCanceled(notExpiredTaskIds),
                        interruptedTasks
                )
        );

        Set<TaskId> taskIdsToInterrupt = ImmutableSet.<TaskId>builder()
                .addAll(expiredTaskIds)
                .addAll(canceledTaskIds)
                .build();
        taskIdsToInterrupt.forEach(taskId -> {
            ActiveTask activeTask = activeTasks.get(taskId);
            activeTask.future().cancel(true);
            interruptedTasks.add(taskId);
            getExpiredTasksCounter(activeTask.taskEntity()).increment();
        });

        expiredInstants.forEach(expiredDateToTask::removeAll);
        removeFromExpiredMap(canceledTaskIds);

        if (!expiredTaskIds.isEmpty()) {
            log.warn("cleanExpiredTasks(): next tasks expired and were interrupted: taskIds=[{}]", expiredTaskIds);
        }
        if (!canceledTaskIds.isEmpty()) {
            log.warn("cleanExpiredTasks(): next tasks have been canceled and were interrupted: taskIds=[{}]", canceledTaskIds);
        }
    }

    private void cleanFinishedTasks() {
        var iterator = activeTasks.entrySet().iterator();
        Set<TaskId> removedTaskIds = Sets.newHashSet();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            //.isDone() doesn't work in case where task has been canceled, it means task can be still alive, but
            //.isDone() return true. We have to wait for real task completion in order not to run it in parallel.
            if (entry.getValue().isCompleted().get()) {
                TaskId taskId = entry.getKey();
                removedTaskIds.add(taskId);
                interruptedTasks.remove(taskId);
                log.info("cleanFinishedTasks(): task=[{}] has been completed", taskId);
                iterator.remove();
            }
        }
        removeFromExpiredMap(removedTaskIds);
    }

    private void removeFromExpiredMap(Set<TaskId> removedTaskIds) {
        removedTaskIds = Sets.newHashSet(removedTaskIds);
        var iterator = expiredDateToTask.entries().iterator();
        while (iterator.hasNext() && !removedTaskIds.isEmpty()) {
            var entry = iterator.next();
            TaskId taskId = entry.getValue();
            if (removedTaskIds.remove(taskId)) {
                iterator.remove();
            }
        }
    }

    /**
     * @noinspection ConstantConditions, UnstableApiUsage
     */
    private void sleep(int taskNumber) throws InterruptedException {
        Integer maxInConfig = workerManagerSettings.getManageDelay().span().upperEndpoint();
        taskNumber = Math.min(taskNumber, maxInConfig);
        Integer delayMs = workerManagerSettings.getManageDelay().get(taskNumber);
        TimeUnit.MILLISECONDS.sleep(delayMs);
    }

    @Override
    public long getCurrentActiveTasks() {
        return activeTasks.size();
    }

    private Timer getManageTimer() {
        return metricHelper.timer("workerManager", "manage", "time");
    }

    private Counter getUnknownTaskCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
                List.of("workerManager", "unknown"),
                List.of(),
                taskEntity
        );
    }

    private Counter getManagedTasks() {
        return metricHelper.counter("workerManager", "managed");
    }

    private Counter getStarvationTasks() {
        return metricHelper.counter("workerManager", "starvation");
    }

    private Counter getExpiredTasksCounter(TaskEntity taskEntity) {
        return metricHelper.counter(List.of("workerManager", "expiredTasks"), Collections.emptyList(), taskEntity);
    }
}
