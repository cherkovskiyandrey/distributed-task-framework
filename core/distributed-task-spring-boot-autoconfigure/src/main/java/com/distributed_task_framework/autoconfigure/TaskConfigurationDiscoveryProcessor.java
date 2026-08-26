package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.annotation.RetryOff;
import com.distributed_task_framework.autoconfigure.annotation.TaskBackoffRetryPolicy;
import com.distributed_task_framework.autoconfigure.annotation.TaskConcurrency;
import com.distributed_task_framework.autoconfigure.annotation.TaskCreationInterceptors;
import com.distributed_task_framework.autoconfigure.annotation.TaskDltEnable;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionInterceptors;
import com.distributed_task_framework.autoconfigure.annotation.TaskFixedRetryPolicy;
import com.distributed_task_framework.autoconfigure.annotation.TaskSchedule;
import com.distributed_task_framework.autoconfigure.annotation.TaskTimeout;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.interceptor.CommonTaskCreationInterceptor;
import com.distributed_task_framework.interceptor.CommonTaskExecutionInterceptor;
import com.distributed_task_framework.interceptor.TaskCreationInterceptor;
import com.distributed_task_framework.interceptor.TaskExecutionInterceptor;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.RetryMode;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.autoconfigure.utils.ReflectionHelper;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TaskConfigurationDiscoveryProcessor {
    public static final BiFunction<TaskSettings, TaskDef<?>, TaskSettings> EMPTY_TASK_SETTINGS_CUSTOMIZER = (taskSettings, taskDef) -> taskSettings;

    DistributedTaskProperties properties;
    DistributedTaskService distributedTaskService;
    ThreadPoolExecutor executor;
    DistributedTaskPropertiesMapper distributedTaskPropertiesMapper;
    DistributedTaskPropertiesMerger distributedTaskPropertiesMerger;
    Collection<Task<?>> tasks;
    RemoteTasks remoteTasks;
    BiFunction<TaskSettings, TaskDef<?>, TaskSettings> taskSettingCustomizer;

    public TaskConfigurationDiscoveryProcessor(DistributedTaskProperties properties,
                                               DistributedTaskService distributedTaskService,
                                               DistributedTaskPropertiesMapper distributedTaskPropertiesMapper,
                                               DistributedTaskPropertiesMerger distributedTaskPropertiesMerger,
                                               Collection<Task<?>> tasks,
                                               RemoteTasks remoteTasks,
                                               BiFunction<TaskSettings, TaskDef<?>, TaskSettings> taskSettingCustomizer) {
        this.properties = properties;
        this.distributedTaskService = distributedTaskService;
        this.distributedTaskPropertiesMapper = distributedTaskPropertiesMapper;
        this.distributedTaskPropertiesMerger = distributedTaskPropertiesMerger;
        this.tasks = tasks;
        this.remoteTasks = remoteTasks;
        this.taskSettingCustomizer = taskSettingCustomizer;
        this.executor = new ThreadPoolExecutor(
            0,
            1,
            1L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
        );
    }

    @SneakyThrows
    @PostConstruct
    public void init() {
        validateDefaultProperties();
        registerLocalTasks();
        registerRemoteTasksFromCode();
        //configurations for unknown local tasks just ignore.
    }

    private void validateDefaultProperties() {
        var defaultProperties = Optional.ofNullable(properties.getTaskPropertiesGroup())
            .map(DistributedTaskProperties.TaskPropertiesGroup::getDefaultProperties)
            .orElse(null);
        if (defaultProperties == null) {
            return;
        }
        var excludedCommonCreation = defaultProperties.getExcludedCommonCreationInterceptors();
        if (excludedCommonCreation != null && !excludedCommonCreation.isEmpty()) {
            throw new TaskConfigurationException(
                "It is prohibited to exclude common interceptors in default-properties, " +
                    "excluded-common-creation-interceptors=[%s]".formatted(excludedCommonCreation)
            );
        }
        var excludedCommonExecution = defaultProperties.getExcludedCommonExecutionInterceptors();
        if (excludedCommonExecution != null && !excludedCommonExecution.isEmpty()) {
            throw new TaskConfigurationException(
                "It is prohibited to exclude common interceptors in default-properties, " +
                    "excluded-common-execution-interceptors=[%s]".formatted(excludedCommonExecution)
            );
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): shutdown started");
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): shutdown completed");
    }

    private void registerLocalTasks() {
        for (Task<?> task : tasks) {
            TaskSettings taskSettings = taskSettingCustomizer.apply(
                buildTaskSettings(task),
                task.getDef()
            );
            distributedTaskService.registerTask(task, taskSettings);
            if (taskSettings.hasCron()) {
                //we have to start recurrent task almost immediately
                //it is important to run async, because otherwise a deadlock will happen in case when
                //there are fewer db connections in the pool. Because ClusterProviderImpl run async
                //updateOwnState in a transaction and try to get nodeStateRepository, which
                //must be created as a bean from spring context, but spring context is blocked until
                //any @PostConstruct is completed. At the same time here we get spring context lock
                //and try to obtain transaction:
                //TaskConfigurationDiscoveryProcessor -> __synchronize(spring factory)__ -> init() -> distributedTaskService.schedule -> taskRepository.findByName -> __getConnection__
                //ClusterProviderImpl -> __TX(getConnection())__ -> updateOwnState() -> __synchronize(spring factory)__ -> nodeStateRepository.findById()
                CompletableFuture.runAsync(() -> {
                        try {
                            distributedTaskService.schedule(task.getDef(), ExecutionContext.empty());
                        } catch (Exception ignore) {
                        }
                    },
                    executor
                );
            }
        }
    }

    private void registerRemoteTasksFromCode() {
        for (TaskDef<?> taskDef : remoteTasks.remoteTasks()) {
            TaskSettings taskSettings = buildRemoteTaskSettings(taskDef, null);
            distributedTaskService.registerRemoteTask(taskDef, taskSettings);
        }

        for (RemoteTaskWithParameters<?> taskWithSettings : remoteTasks.remoteTasksWithSettings()) {
            TaskDef<?> taskDef = taskWithSettings.getTaskDef();
            TaskSettings taskSettingsFromCode = taskWithSettings.getTaskSettings();
            TaskSettings taskSettings = buildRemoteTaskSettings(taskDef, taskSettingsFromCode);
            distributedTaskService.registerRemoteTask(taskDef, taskSettings);
        }
    }

    public TaskSettings buildTaskSettings(Task<?> task) {
        return buildTaskSettingsBase(task.getDef(), fillCustomProperties(task));
    }

    private TaskSettings buildRemoteTaskSettings(TaskDef<?> taskDef, @Nullable TaskSettings customCodeTaskSettings) {
        var customCodeTaskProperties = distributedTaskPropertiesMapper.map(customCodeTaskSettings);
        customCodeTaskProperties = customCodeTaskProperties != null ?
            customCodeTaskProperties :
            new DistributedTaskProperties.TaskProperties();
        return buildTaskSettingsBase(taskDef, customCodeTaskProperties);
    }

    private TaskSettings buildTaskSettingsBase(TaskDef<?> taskDef,
                                               DistributedTaskProperties.TaskProperties customCodeTaskProperties) {
        var taskPropertiesGroup = Optional.ofNullable(properties.getTaskPropertiesGroup());

        var defaultCodeTaskProperties = distributedTaskPropertiesMapper.map(TaskSettings.DEFAULT);

        var defaultConfTaskProperties = taskPropertiesGroup
            .map(DistributedTaskProperties.TaskPropertiesGroup::getDefaultProperties)
            .orElse(null);
        var customConfTaskProperties = taskPropertiesGroup
            .map(DistributedTaskProperties.TaskPropertiesGroup::getTaskProperties)
            .map(taskProperties -> taskProperties.get(taskDef.getTaskName()))
            .orElse(null);

        //interceptor lists have to be composed before the merge because merging mutates properties
        List<Class<? extends TaskCreationInterceptor>> creationInterceptors = composeCreationInterceptors(
            defaultConfTaskProperties,
            customCodeTaskProperties,
            customConfTaskProperties
        );
        List<Class<? extends TaskExecutionInterceptor>> executionInterceptors = composeExecutionInterceptors(
            defaultConfTaskProperties,
            customCodeTaskProperties,
            customConfTaskProperties
        );
        List<Class<? extends CommonTaskCreationInterceptor>> excludedCommonCreationInterceptors = composeExcludedCommonCreationInterceptors(
            customCodeTaskProperties,
            customConfTaskProperties
        );
        List<Class<? extends CommonTaskExecutionInterceptor>> excludedCommonExecutionInterceptors = composeExcludedCommonExecutionInterceptors(
            customCodeTaskProperties,
            customConfTaskProperties
        );

        var defaultTaskProperties = distributedTaskPropertiesMerger.merge(
            defaultCodeTaskProperties,
            defaultConfTaskProperties
        );
        var customTaskProperties = distributedTaskPropertiesMerger.merge(
            customCodeTaskProperties,
            customConfTaskProperties
        );
        var taskProperties = distributedTaskPropertiesMerger.merge(
            defaultTaskProperties,
            customTaskProperties
        );

        TaskSettings taskSettings = distributedTaskPropertiesMapper.map(taskProperties);
        return taskSettings.toBuilder()
            .creationInterceptors(creationInterceptors)
            .executionInterceptors(executionInterceptors)
            .excludedCommonCreationInterceptors(excludedCommonCreationInterceptors)
            .excludedCommonExecutionInterceptors(excludedCommonExecutionInterceptors)
            .build();
    }

    private static List<Class<? extends TaskCreationInterceptor>> composeCreationInterceptors(@Nullable DistributedTaskProperties.TaskProperties defaultConf,
                                                                                               DistributedTaskProperties.TaskProperties customCode,
                                                                                               @Nullable DistributedTaskProperties.TaskProperties customConf) {
        return Stream.of(defaultConf, customCode, customConf)
            .filter(Objects::nonNull)
            .map(DistributedTaskProperties.TaskProperties::getCreationInterceptors)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .distinct()
            .toList();
    }

    private static List<Class<? extends TaskExecutionInterceptor>> composeExecutionInterceptors(@Nullable DistributedTaskProperties.TaskProperties defaultConf,
                                                                                                DistributedTaskProperties.TaskProperties customCode,
                                                                                                @Nullable DistributedTaskProperties.TaskProperties customConf) {
        return Stream.of(defaultConf, customCode, customConf)
            .filter(Objects::nonNull)
            .map(DistributedTaskProperties.TaskProperties::getExecutionInterceptors)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .distinct()
            .toList();
    }

    //exclusions in default-properties are prohibited, see validateDefaultProperties()
    private static List<Class<? extends CommonTaskCreationInterceptor>> composeExcludedCommonCreationInterceptors(DistributedTaskProperties.TaskProperties customCode,
                                                                                                                  @Nullable DistributedTaskProperties.TaskProperties customConf) {
        return Stream.of(customCode, customConf)
            .filter(Objects::nonNull)
            .map(DistributedTaskProperties.TaskProperties::getExcludedCommonCreationInterceptors)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .distinct()
            .toList();
    }

    private static List<Class<? extends CommonTaskExecutionInterceptor>> composeExcludedCommonExecutionInterceptors(DistributedTaskProperties.TaskProperties customCode,
                                                                                                                    @Nullable DistributedTaskProperties.TaskProperties customConf) {
        return Stream.of(customCode, customConf)
            .filter(Objects::nonNull)
            .map(DistributedTaskProperties.TaskProperties::getExcludedCommonExecutionInterceptors)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .distinct()
            .toList();
    }

    private DistributedTaskProperties.TaskProperties fillCustomProperties(Task<?> task) {
        var taskProperties = new DistributedTaskProperties.TaskProperties();
        fillSchedule(task, taskProperties);
        fillConcurrency(task, taskProperties);
        fillExecutionGuarantees(task, taskProperties);
        fillDltMode(task, taskProperties);
        fillRetryMode(task, taskProperties);
        fillTaskTimeout(task, taskProperties);
        fillCreationInterceptors(task, taskProperties);
        fillExecutionInterceptors(task, taskProperties);
        return taskProperties;
    }

    private void fillCreationInterceptors(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        ReflectionHelper.findAnnotation(task, TaskCreationInterceptors.class)
            .ifPresent(annotation -> {
                if (annotation.value().length > 0) {
                    taskProperties.setCreationInterceptors(List.of(annotation.value()));
                }
                if (annotation.excludedCommon().length > 0) {
                    taskProperties.setExcludedCommonCreationInterceptors(List.of(annotation.excludedCommon()));
                }
            });
    }

    private void fillExecutionInterceptors(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        ReflectionHelper.findAnnotation(task, TaskExecutionInterceptors.class)
            .ifPresent(annotation -> {
                if (annotation.value().length > 0) {
                    taskProperties.setExecutionInterceptors(List.of(annotation.value()));
                }
                if (annotation.excludedCommon().length > 0) {
                    taskProperties.setExcludedCommonExecutionInterceptors(List.of(annotation.excludedCommon()));
                }
            });
    }

    private void fillTaskTimeout(Task<?> task, DistributedTaskProperties.TaskProperties taskSettings) {
        Optional<TaskTimeout> taskTimeoutOpt = ReflectionHelper.findAnnotation(task, TaskTimeout.class);
        taskTimeoutOpt.ifPresent(taskTimeout -> {
            if (isNotBlank(taskTimeout.value())) {
                taskSettings.setTimeout(Duration.parse(taskTimeout.value()));
            }
        });
    }

    private void fillRetryMode(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        Optional<TaskFixedRetryPolicy> taskFixedRetryPolicy = ReflectionHelper.findAnnotation(task, TaskFixedRetryPolicy.class);
        Optional<TaskBackoffRetryPolicy> taskBackoffRetryPolicy = ReflectionHelper.findAnnotation(task, TaskBackoffRetryPolicy.class);
        Optional<RetryOff> retryOffPolicy = ReflectionHelper.findAnnotation(task, RetryOff.class);
        if ((taskFixedRetryPolicy.isPresent() && taskBackoffRetryPolicy.isPresent()) ||
            (taskFixedRetryPolicy.isPresent() && retryOffPolicy.isPresent()) ||
            (taskBackoffRetryPolicy.isPresent() && retryOffPolicy.isPresent())
        ) {
            throw new TaskConfigurationException("Only one retry policy is allowed. TaskDef=[%s]".formatted(task.getDef()));
        }
        taskFixedRetryPolicy.ifPresent(retryPolicy -> fillFixedRetryMode(retryPolicy, taskProperties));
        taskBackoffRetryPolicy.ifPresent(retryPolicy -> fillBackoffRetryMode(retryPolicy, taskProperties));
        retryOffPolicy.ifPresent(retryPolicy -> fillOffRetryMode(taskProperties));
    }

    private void fillOffRetryMode(DistributedTaskProperties.TaskProperties taskProperties) {
        DistributedTaskProperties.Retry retry = new DistributedTaskProperties.Retry();
        taskProperties.setRetry(retry);
        retry.setRetryMode(RetryMode.OFF.toString());
    }

    private void fillBackoffRetryMode(TaskBackoffRetryPolicy retryPolicy, DistributedTaskProperties.TaskProperties taskProperties) {
        DistributedTaskProperties.Retry retry = new DistributedTaskProperties.Retry();
        taskProperties.setRetry(retry);
        DistributedTaskProperties.Backoff backoff = new DistributedTaskProperties.Backoff();
        retry.setBackoff(backoff);

        retry.setRetryMode(RetryMode.BACKOFF.toString());
        if (StringUtils.hasText(retryPolicy.initialDelay())) {
            backoff.setInitialDelay(Duration.parse(retryPolicy.initialDelay()));
        }
        if (StringUtils.hasText(retryPolicy.delayPeriod())) {
            backoff.setDelayPeriod(Duration.parse(retryPolicy.delayPeriod()));
        }
        if (retryPolicy.maxRetries() > 0) {
            backoff.setMaxRetries(retryPolicy.maxRetries());
        }
        if (StringUtils.hasText(retryPolicy.maxDelay())) {
            backoff.setMaxDelay(Duration.parse(retryPolicy.maxDelay()));
        }
    }

    private void fillFixedRetryMode(TaskFixedRetryPolicy retryPolicy, DistributedTaskProperties.TaskProperties taskProperties) {
        DistributedTaskProperties.Retry retry = new DistributedTaskProperties.Retry();
        taskProperties.setRetry(retry);
        DistributedTaskProperties.Fixed fixed = new DistributedTaskProperties.Fixed();
        retry.setFixed(fixed);

        retry.setRetryMode(RetryMode.FIXED.toString());
        if (StringUtils.hasText(retryPolicy.delay())) {
            fixed.setDelay(Duration.parse(retryPolicy.delay()));
        }
        if (retryPolicy.number() > 0) {
            fixed.setMaxNumber(retryPolicy.number());
        }
        if (StringUtils.hasText(retryPolicy.maxInterval())) {
            fixed.setMaxInterval(Duration.parse(retryPolicy.maxInterval()));
        }
    }

    private void fillDltMode(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        ReflectionHelper.findAnnotation(task, TaskDltEnable.class)
            .ifPresent(taskDltEnable -> taskProperties.setDltEnabled(taskDltEnable.isEnabled()));
    }

    private void fillExecutionGuarantees(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        ReflectionHelper.findAnnotation(task, TaskExecutionGuarantees.class)
            .ifPresent(executionGuarantees ->
                taskProperties.setExecutionGuarantees(executionGuarantees.value().toString())
            );
    }

    private void fillConcurrency(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        ReflectionHelper.findAnnotation(task, TaskConcurrency.class)
            .ifPresent(taskConcurrency -> {
                if (taskConcurrency.maxParallelInCluster() > CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS) {
                    taskProperties.setMaxParallelInCluster(taskConcurrency.maxParallelInCluster());
                }
                if (taskConcurrency.maxParallelInNode() > CommonSettings.PlannerSettings.UNLIMITED_PARALLEL_TASKS) {
                    taskProperties.setMaxParallelInNode(taskConcurrency.maxParallelInNode());
                }
            });
    }

    private void fillSchedule(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        ReflectionHelper.findAnnotation(task, TaskSchedule.class)
            .ifPresent(mergedAnnotation -> taskProperties.setCron(mergedAnnotation.cron()));
    }
}
