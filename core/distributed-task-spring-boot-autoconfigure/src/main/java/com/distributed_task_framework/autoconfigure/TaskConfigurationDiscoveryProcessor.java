package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.annotation.RetryOff;
import com.distributed_task_framework.autoconfigure.annotation.TaskBackoffRetryPolicy;
import com.distributed_task_framework.autoconfigure.annotation.TaskConcurrency;
import com.distributed_task_framework.autoconfigure.annotation.TaskDltEnable;
import com.distributed_task_framework.autoconfigure.annotation.TaskExecutionGuarantees;
import com.distributed_task_framework.autoconfigure.annotation.TaskFixedRetryPolicy;
import com.distributed_task_framework.autoconfigure.annotation.TaskSchedule;
import com.distributed_task_framework.autoconfigure.annotation.TaskTimeout;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.autoconfigure.utils.ReflectionHelper;
import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.RetryMode;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.utils.DistributedTaskServiceLifecycle;
import com.google.common.collect.Lists;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TaskConfigurationDiscoveryProcessor implements DistributedTaskServiceLifecycle {
    public static final BiFunction<TaskSettings, TaskDef<?>, TaskSettings> EMPTY_TASK_SETTINGS_CUSTOMIZER = (taskSettings, taskDef) -> taskSettings;

    DistributedTaskProperties properties;
    DistributedTaskService distributedTaskService;
    DistributedTaskPropertiesMapper distributedTaskPropertiesMapper;
    DistributedTaskPropertiesMerger distributedTaskPropertiesMerger;
    Collection<Task<?>> tasks;
    RemoteTasks remoteTasks;
    CopyOnWriteArrayList<TaskDef<?>> cronTasksToStart;
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
        this.cronTasksToStart = Lists.newCopyOnWriteArrayList();
    }

    @Override
    public void init() throws Exception {
        registerLocalTasks();
        registerRemoteTasksFromCode();
        //configurations for unknown local tasks just ignore.
    }

    @Override
    public void start() throws Exception {
        for (var taskDef : cronTasksToStart) {
            distributedTaskService.schedule(taskDef, ExecutionContext.empty());
        }
    }

    private void registerLocalTasks() {
        for (Task<?> task : tasks) {
            TaskSettings taskSettings = taskSettingCustomizer.apply(
                buildTaskSettings(task),
                task.getDef()
            );
            distributedTaskService.registerTask(task, taskSettings);

            if (taskSettings.hasCron()) {
                cronTasksToStart.add(task.getDef());
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

        return distributedTaskPropertiesMapper.map(taskProperties);
    }

    private DistributedTaskProperties.TaskProperties fillCustomProperties(Task<?> task) {
        var taskProperties = new DistributedTaskProperties.TaskProperties();
        fillSchedule(task, taskProperties);
        fillConcurrency(task, taskProperties);
        fillExecutionGuarantees(task, taskProperties);
        fillDltMode(task, taskProperties);
        fillRetryMode(task, taskProperties);
        fillTaskTimeout(task, taskProperties);
        return taskProperties;
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
