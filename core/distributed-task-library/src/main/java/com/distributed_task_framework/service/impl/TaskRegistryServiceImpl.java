package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.utils.DistributedTaskCacheSettings;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;
import com.distributed_task_framework.persistence.repository.RegisteredTaskRepository;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.utils.DistributedTaskCache;
import com.distributed_task_framework.utils.DistributedTaskCacheManager;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.common.RemoteStubTask;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskRegistryServiceImpl implements TaskRegistryService {
    private final static String REGISTERED_LOCAL_TASK_IN_CLUSTER_CACHE = "registeredLocalTaskInClusterCache";
    private final static String REGISTERED_LOCAL_TASK_IN_CLUSTER_KEY = "registeredLocalTaskInClusterKey";

    ConcurrentMap<TaskDef<?>, RegisteredTask<?>> registeredTasks = Maps.newConcurrentMap();
    CommonSettings commonSettings;
    RegisteredTaskRepository registeredTaskRepository;
    PlatformTransactionManager transactionManager;
    DistributedTaskCache<Map<UUID, Set<String>>> registeredLocalTaskInClusterCache;
    ClusterProvider clusterProvider;
    CronService cronService;
    ScheduledExecutorService scheduledExecutorService;

    public TaskRegistryServiceImpl(CommonSettings commonSettings,
                                   RegisteredTaskRepository registeredTaskRepository,
                                   PlatformTransactionManager transactionManager,
                                   DistributedTaskCacheManager distributedTaskCacheManager,
                                   ClusterProvider clusterProvider,
                                   CronService cronService) {
        this.commonSettings = commonSettings;
        this.registeredTaskRepository = registeredTaskRepository;
        this.transactionManager = transactionManager;
        this.registeredLocalTaskInClusterCache = distributedTaskCacheManager.getOrCreateCache(
            REGISTERED_LOCAL_TASK_IN_CLUSTER_CACHE,
            DistributedTaskCacheSettings.builder()
                .expireAfterWrite(Duration.ofMillis(commonSettings.getRegistrySettings().getCacheExpirationMs()))
                .build()
        );
        this.clusterProvider = clusterProvider;
        this.cronService = cronService;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setDaemon(false)
            .setNameFormat("node-st-upd-%d")
            .setUncaughtExceptionHandler((t, e) -> {
                log.error("nodeStateUpdaterErrorHandler(): error when try to update node state", e);
                ReflectionUtils.rethrowRuntimeException(e);
            })
            .build()
        );
    }

    @PostConstruct
    public void init() {
        log.info("init(): nodeId=[{}]", clusterProvider.nodeId());
        scheduledExecutorService.scheduleWithFixedDelay(
            ExecutorUtils.wrapRepeatableRunnable(this::publishOrUpdateTasksInCluster),
            commonSettings.getRegistrySettings().getUpdateInitialDelayMs(),
            commonSettings.getRegistrySettings().getUpdateFixedDelayMs(),
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * @noinspection ResultOfMethodCallIgnored
     */
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): nodeId=[{}] shutdown started", clusterProvider.nodeId());
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        unregisterItself();
        log.info("shutdown(): nodeId=[{}] shutdown completed", clusterProvider.nodeId());
    }

    @Override
    public <T> void registerTask(Task<T> task, TaskSettings taskSettings) {
        TaskDef<T> taskDef = setAppIfRequired(task.getDef());
        validate(taskDef, taskSettings, false);
        RegisteredTask<?> registeredTask = registeredTasks.putIfAbsent(taskDef, RegisteredTask.of(task, taskSettings));
        if (registeredTask != null) {
            throw new TaskConfigurationException("task by taskDef=[%s] already registered".formatted(taskDef));
        }
        log.info("registryTask(): taskDef=[{}], taskParameters=[{}]", taskDef, taskSettings);
    }

    @Override
    public <T> void registerRemoteTask(TaskDef<T> taskDef, TaskSettings taskSettings) {
        validate(taskDef, taskSettings, true);
        RegisteredTask<?> registeredTask = registeredTasks.putIfAbsent(
            taskDef,
            RegisteredTask.of(RemoteStubTask.stubFor(taskDef), taskSettings)
        );
        if (registeredTask != null) {
            throw new TaskConfigurationException("task by taskDef=[%s] already registered".formatted(taskDef));
        }
        log.info("registryRemoteTask(): taskDef=[{}], taskParameters=[{}]", taskDef, taskSettings);
    }

    @Override
    public Optional<TaskSettings> getLocalTaskParameters(String taskName) {
        return Optional.ofNullable(registeredTasks.get(toLocalTaskDef(taskName))).map(RegisteredTask::getTaskSettings);
    }

    @Override
    public <T> Optional<TaskSettings> getTaskParameters(TaskDef<T> taskDef) {
        return Optional.ofNullable(registeredTasks.get(setAppIfRequired(taskDef))).map(RegisteredTask::getTaskSettings);
    }

    @Override
    public boolean isLocalTaskRegistered(String taskName) {
        return registeredTasks.containsKey(toLocalTaskDef(taskName));
    }

    @Override
    public <T> boolean isTaskRegistered(TaskDef<T> taskDef) {
        return registeredTasks.containsKey(setAppIfRequired(taskDef));
    }

    private TaskDef<?> toLocalTaskDef(String taskName) {
        return TaskDef.publicTaskDef(commonSettings.getAppName(), taskName, Void.class);
    }

    @Override
    public boolean unregisterLocalTask(String taskName) {
        if (registeredTasks.remove(toLocalTaskDef(taskName)) != null) {
            log.info("unregisterLocalTask(): taskName=[{}]", taskName);
            return true;
        }
        return false;
    }

    @Override
    public <T> boolean unregisterTask(TaskDef<T> taskDef) {
        if (registeredTasks.remove(setAppIfRequired(taskDef)) != null) {
            log.info("unregisterTask(): taskDef=[{}]", taskDef);
            return true;
        }
        return false;
    }

    /**
     * @noinspection unchecked
     */
    @Override
    public <T> Optional<RegisteredTask<T>> getRegisteredLocalTask(String taskName) {
        return Optional.ofNullable((RegisteredTask<T>) registeredTasks.get(toLocalTaskDef(taskName)));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<TaskDef<T>> getRegisteredLocalTaskDef(String taskName) {
        return getRegisteredLocalTask(taskName)
            .map(RegisteredTask::getTask)
            .map(objectTask -> (TaskDef<T>) objectTask.getDef());
    }

    /**
     * @noinspection unchecked
     */
    @Override
    public <T> Optional<RegisteredTask<T>> getRegisteredTask(TaskDef<T> taskDef) {
        return Optional.ofNullable((RegisteredTask<T>) registeredTasks.get(setAppIfRequired(taskDef)));
    }

    @Override
    public Map<UUID, Set<String>> getRegisteredLocalTaskInCluster() {
        return registeredLocalTaskInClusterCache.get(
            REGISTERED_LOCAL_TASK_IN_CLUSTER_KEY,
            this::readRegisteredLocalTaskInCluster
        );
    }

    @Override
    public boolean hasClusterRegisteredTaskByName(String name) {
        return getRegisteredLocalTaskInCluster().values().stream()
            .anyMatch(set -> set.contains(name));
    }

    @SuppressWarnings("DataFlowIssue")
    private Map<UUID, Set<String>> readRegisteredLocalTaskInCluster() {
        return Lists.newArrayList(registeredTaskRepository.findAll()).stream()
            .collect(Collectors.groupingBy(
                    RegisteredTaskEntity::getNodeStateId,
                    Collectors.mapping(
                        RegisteredTaskEntity::getTaskName,
                        Collectors.toSet()
                    )
                )
            );
    }

    @VisibleForTesting
    void publishOrUpdateTasksInCluster() {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.executeWithoutResult(status -> doPublishOrUpdateTasksInCluster());
    }

    private void doPublishOrUpdateTasksInCluster() {
        if (!clusterProvider.isNodeRegistered()) {
            return;
        }
        var nodeId = clusterProvider.nodeId();
        var publishedCurrentTasks = Sets.newHashSet(registeredTaskRepository.findByNodeStateId(nodeId));
        var registeredCurrentTasks = this.registeredTasks.keySet().stream()
            .filter(this::isLocal)
            .map(taskDef -> RegisteredTaskEntity.builder()
                .nodeStateId(nodeId)
                .taskName(taskDef.getTaskName())
                .build()
            )
            .collect(Collectors.toSet());

        boolean hasToBeUpdated = registeredCurrentTasks.size() != publishedCurrentTasks.size() ||
            Sets.intersection(registeredCurrentTasks, publishedCurrentTasks).size() != publishedCurrentTasks.size();
        if (hasToBeUpdated) {
            log.info(
                "nodeStateUpdater(): threadId=[{}] set of registered tasks changed from=[{}], to=[{}]",
                Thread.currentThread().getId(),
                publishedCurrentTasks,
                registeredCurrentTasks
            );
            registeredTaskRepository.deleteAllByNodeStateId(nodeId);
            registeredTaskRepository.saveOrUpdateBatch(registeredCurrentTasks);
        }
    }

    private boolean isLocal(TaskDef<?> taskDef) {
        return commonSettings.getAppName().equals(taskDef.getAppName());
    }

    private void unregisterItself() {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.executeWithoutResult(status -> {
            UUID nodeId = clusterProvider.nodeId();
            registeredTaskRepository.deleteAllByNodeStateId(nodeId);
        });
    }

    private <T> TaskDef<T> setAppIfRequired(TaskDef<T> taskDef) {
        return StringUtils.hasText(taskDef.getAppName()) ? taskDef : taskDef.withAppName(commonSettings.getAppName());
    }

    private void validate(TaskDef<?> taskDef, TaskSettings taskSettings, boolean remote) {
        if (!StringUtils.hasText(taskDef.getAppName())) {
            throw new TaskConfigurationException("Unknown taskDef=[%s]".formatted(taskDef));
        }
        if (!remote && !commonSettings.getAppName().equals(taskDef.getAppName())) {
            throw new TaskConfigurationException("Unknown local taskDef=[%s], may be remote?".formatted(taskDef));
        }
        if (remote && !commonSettings.getDeliveryManagerSettings().getRemoteApps().getAppToUrl().containsKey(taskDef.getAppName())) {
            throw new TaskConfigurationException("Unknown remote app for taskDef=[%s]".formatted(taskDef));
        }

        if (StringUtils.hasText(taskSettings.getCron())
            && !cronService.isValidCronOrDurationExpression(taskSettings.getCron())) {
            throw new TaskConfigurationException("Cron expression=[%s] is invalid".formatted(taskSettings.getCron()));
        }
        //todo: maxParallel in cluster and in node
        //todo: check validation of retry parameters
        //todo: check tables exist
        //todo: check kafka topics exist
    }
}
