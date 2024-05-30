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
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.RetryMode;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.StringUtils;

import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TaskConfigurationDiscoveryProcessor {
    DistributedTaskProperties properties;
    DistributedTaskService distributedTaskService;
    DistributedTaskPropertiesMapper distributedTaskPropertiesMapper;
    Collection<Task<?>> tasks;
    RemoteTasks remoteTasks;

    public TaskConfigurationDiscoveryProcessor(DistributedTaskProperties properties,
                                               DistributedTaskService distributedTaskService,
                                               DistributedTaskPropertiesMapper distributedTaskPropertiesMapper,
                                               Collection<Task<?>> tasks,
                                               RemoteTasks remoteTasks) {
        this.properties = properties;
        this.distributedTaskService = distributedTaskService;
        this.distributedTaskPropertiesMapper = distributedTaskPropertiesMapper;
        this.tasks = tasks;
        this.remoteTasks = remoteTasks;
    }

    @SneakyThrows
    @PostConstruct
    public void init() {
        registerLocalTasks();
        registerRemoteTasksFromCode();
        //configurations for unknown local tasks just ignore.
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

    private TaskSettings buildRemoteTaskSettings(TaskDef<?> taskDef, @Nullable TaskSettings customSettings) {
        var taskSettingGroup = Optional.ofNullable(properties.getTaskPropertiesGroup());

        var defaultTaskProperties = distributedTaskPropertiesMapper.map(TaskSettings.DEFAULT);
        var defaultConfTaskProperties = taskSettingGroup
                .map(DistributedTaskProperties.TaskPropertiesGroup::getDefaultProperties)
                .orElse(null);

        var customTaskProperties = distributedTaskPropertiesMapper.map(customSettings);
        var customConfTaskProperties = taskSettingGroup
                .map(DistributedTaskProperties.TaskPropertiesGroup::getTaskProperties)
                .map(taskProperties -> taskProperties.get(taskDef.getTaskName()))
                .orElse(null);

        defaultTaskProperties = defaultConfTaskProperties != null ?
                distributedTaskPropertiesMapper.merge(defaultTaskProperties, defaultConfTaskProperties) :
                defaultTaskProperties;

        if (customTaskProperties != null && customConfTaskProperties != null) {
            customTaskProperties = distributedTaskPropertiesMapper.merge(customTaskProperties, customConfTaskProperties);
        } else if (customConfTaskProperties != null) {
            customTaskProperties = customConfTaskProperties;
        }

        var taskProperties = customTaskProperties != null ?
                distributedTaskPropertiesMapper.merge(defaultTaskProperties, customTaskProperties) :
                defaultTaskProperties;

        return distributedTaskPropertiesMapper.map(taskProperties);
    }

    private void registerLocalTasks() throws Exception {
        for (Task<?> task : tasks) {
            TaskSettings taskSettings = buildTaskSettings(task);
            distributedTaskService.registerTask(task, taskSettings);
            if (taskSettings.hasCron()) {
                //we have to start recurrent task immediately
                distributedTaskService.schedule(task.getDef(), ExecutionContext.empty());
            }
        }
    }

    public TaskSettings buildTaskSettings(Task<?> task) {
        var taskSettingGroup = Optional.ofNullable(properties.getTaskPropertiesGroup());

        var defaultSettings = distributedTaskPropertiesMapper.map(TaskSettings.DEFAULT);
        var customSettings = fillCustomSettings(task);

        var defaultConfSettings = taskSettingGroup
                .map(DistributedTaskProperties.TaskPropertiesGroup::getDefaultProperties)
                .orElse(null);
        var customConfSettings = taskSettingGroup
                .map(DistributedTaskProperties.TaskPropertiesGroup::getTaskProperties)
                .map(taskProperties -> taskProperties.get(task.getDef().getTaskName()))
                .orElse(null);

        defaultSettings = defaultConfSettings != null ?
                distributedTaskPropertiesMapper.merge(defaultSettings, defaultConfSettings) :
                defaultSettings;

        customSettings = customConfSettings != null ?
                distributedTaskPropertiesMapper.merge(customSettings, customConfSettings) :
                customSettings;

        var taskSettings = distributedTaskPropertiesMapper.merge(defaultSettings, customSettings);
        return distributedTaskPropertiesMapper.map(taskSettings);
    }

    private DistributedTaskProperties.TaskProperties fillCustomSettings(Task<?> task) {
        var taskSettings = new DistributedTaskProperties.TaskProperties();
        fillSchedule(task, taskSettings);
        fillConcurrency(task, taskSettings);
        fillExecutionGuarantees(task, taskSettings);
        fillDltMode(task, taskSettings);
        fillRetryMode(task, taskSettings);
        fillTaskTimeout(task, taskSettings);
        return taskSettings;
    }

    private void fillTaskTimeout(Task<?> task, DistributedTaskProperties.TaskProperties taskSettings) {
        Optional<TaskTimeout> taskTimeoutOpt = findAnnotation(task, TaskTimeout.class);
        taskTimeoutOpt.ifPresent(taskTimeout -> {
            if (isNotBlank(taskTimeout.value())) {
                taskSettings.setTimeout(Duration.parse(taskTimeout.value()));
            }
        });
    }

    private void fillRetryMode(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        Optional<TaskFixedRetryPolicy> taskFixedRetryPolicy = findAnnotation(task, TaskFixedRetryPolicy.class);
        Optional<TaskBackoffRetryPolicy> taskBackoffRetryPolicy = findAnnotation(task, TaskBackoffRetryPolicy.class);
        Optional<RetryOff> retryOffPolicy = findAnnotation(task, RetryOff.class);
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
            fixed.setDelay(Duration.parse(retryPolicy.maxInterval()));
        }
    }

    private void fillDltMode(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        findAnnotation(task, TaskDltEnable.class)
                .ifPresent(taskDltEnable -> taskProperties.setDltEnabled(taskDltEnable.isEnabled()));
    }

    private void fillExecutionGuarantees(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        findAnnotation(task, TaskExecutionGuarantees.class)
                .ifPresent(executionGuarantees ->
                        taskProperties.setExecutionGuarantees(executionGuarantees.value().toString())
                );
    }

    private void fillConcurrency(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        findAnnotation(task, TaskConcurrency.class)
                .ifPresent(taskConcurrency -> {
                    if (taskConcurrency.maxParallelInCluster() > 0) {
                        taskProperties.setMaxParallelInCluster(taskConcurrency.maxParallelInCluster());
                    }
                });
    }

    private void fillSchedule(Task<?> task, DistributedTaskProperties.TaskProperties taskProperties) {
        findAnnotation(task, TaskSchedule.class)
                .ifPresent(mergedAnnotation -> taskProperties.setCron(mergedAnnotation.cron()));
    }

    /**
     * @noinspection unchecked
     */
    private <A extends Annotation> Optional<A> findAnnotation(Task<?> task, Class<A> annotationCls) {
        A mergedAnnotation = AnnotatedElementUtils.getMergedAnnotation(task.getClass(), annotationCls);
        if (mergedAnnotation != null) {
            return Optional.of(mergedAnnotation);
        }
        Class<Task<?>> targetClass = (Class<Task<?>>) AopUtils.getTargetClass(task);
        return Optional.ofNullable(AnnotatedElementUtils.getMergedAnnotation(targetClass, annotationCls));
    }
}
