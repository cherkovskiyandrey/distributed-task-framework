package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.exception.InvalidOperationException;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.repository.TaskExtendedRepository;
import com.distributed_task_framework.service.internal.CompletionService;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class CompletionServiceImpl implements CompletionService {
    CommonSettings commonSettings;
    TaskExtendedRepository taskExtendedRepository;
    WorkerContextManager workerContextManager;
    ScheduledExecutorService scheduledExecutorService;
    ConcurrentMap<UUID, CompletableFuture<Void>> registeredWorkflows = Maps.newConcurrentMap();
    ConcurrentMap<UUID, CompletableFuture<Void>> registeredTaskIds = Maps.newConcurrentMap();

    public CompletionServiceImpl(CommonSettings commonSettings,
                                 TaskExtendedRepository taskExtendedRepository,
                                 WorkerContextManager workerContextManager) {
        this.commonSettings = commonSettings;
        this.taskExtendedRepository = taskExtendedRepository;
        this.workerContextManager = workerContextManager;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("tsk-completion")
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("scheduleWatchdog(): error when handle completion of tasks", e);
                            ReflectionUtils.rethrowRuntimeException(e);
                        })
                        .build()
        );
    }

    @PostConstruct
    public void init() {
        scheduledExecutorService.scheduleWithFixedDelay(
                ExecutorUtils.wrapRepeatableRunnable(this::handle),
                commonSettings.getCompletionSettings().getHandlerInitialDelay().toMillis(),
                commonSettings.getCompletionSettings().getHandlerFixedDelay().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): completion handler shutdown started");
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): completion handler shutdown completed");
    }

    @Override
    public void waitCompletionAllWorkflow(UUID workflowId) throws TimeoutException, InterruptedException {
        waitCompletionAllWorkflow(workflowId, commonSettings.getCompletionSettings().getDefaultWorkflowTimeout());
    }

    public void waitCompletionAllWorkflow(UUID workflowId, Duration timeout) throws TimeoutException, InterruptedException {
        checkPermission();
        var future = registeredWorkflows.computeIfAbsent(workflowId, k -> new CompletableFuture<>());
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) { //impossible case in our case
            throw new RuntimeException(e);
        }
    }

    @Override
    public void waitCompletion(TaskId taskId) throws TimeoutException, InterruptedException {
        waitCompletion(taskId, commonSettings.getCompletionSettings().getDefaultTaskTimeout());
    }

    @Override
    public void waitCompletion(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException {
        checkPermission();
        var future = registeredTaskIds.computeIfAbsent(taskId.getId(), k -> new CompletableFuture<>());
        try {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) { //impossible case in our case
            throw new RuntimeException(e);
        }
    }

    private void checkPermission() {
        boolean isInContext = workerContextManager.getCurrentContext().isPresent();
        if (isInContext) {
            throw new InvalidOperationException("It is prohibited to wait for completion of task/workflow from other task");
        }
    }

    @VisibleForTesting
    void handle() {
        handleBase(registeredTaskIds, taskExtendedRepository::filterExistedTaskIds);
        handleBase(registeredWorkflows, taskExtendedRepository::filterExistedWorkflowIds);
    }

    private void handleBase(ConcurrentMap<UUID, CompletableFuture<Void>> idMap,
                            Function<Set<UUID>, Set<UUID>> filter) {
        var requestedIds = idMap.keySet();
        if (requestedIds.isEmpty()) {
            return;
        }

        var existedIds = filter.apply(requestedIds);
        var completedOrNotExistedIds = Sets.difference(requestedIds, existedIds);
        completedOrNotExistedIds.forEach(id ->
                //use compute in order to protect from parallel registration of feature by the same key
                idMap.compute(
                        id,
                        (k, feature) -> {
                            if (feature != null) {
                                feature.complete(null);
                            }
                            return null; //remove mapping
                        })
        );
    }
}
