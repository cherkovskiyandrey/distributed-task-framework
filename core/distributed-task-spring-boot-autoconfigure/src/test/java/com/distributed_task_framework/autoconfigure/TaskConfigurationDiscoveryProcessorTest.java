package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMapper;
import com.distributed_task_framework.autoconfigure.mapper.DistributedTaskPropertiesMerger;
import com.distributed_task_framework.autoconfigure.tasks.CustomTaskWithOffRetry;
import com.distributed_task_framework.autoconfigure.tasks.CustomizedTask;
import com.distributed_task_framework.autoconfigure.tasks.DefaultTask;
import com.distributed_task_framework.autoconfigure.tasks.SimpleCronCustomizedTask;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.Backoff;
import com.distributed_task_framework.settings.Fixed;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.RetryMode;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.distributed_task_framework.autoconfigure.TaskConfigurationDiscoveryProcessor.EMPTY_TASK_SETTINGS_CUSTOMIZER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ActiveProfiles("test")
@SpringBootTest
@ContextConfiguration(classes = MappersConfiguration.class)
@FieldDefaults(level = AccessLevel.PRIVATE)
class TaskConfigurationDiscoveryProcessorTest {
    @SpyBean
    DistributedTaskPropertiesMapper distributedTaskPropertiesMapper;
    @SpyBean
    DistributedTaskPropertiesMerger distributedTaskPropertiesMerger;
    @MockBean
    DistributedTaskProperties properties;
    @MockBean
    DistributedTaskService distributedTaskService;
    final Collection<Task<?>> tasks = Lists.newArrayList();
    @MockBean
    RemoteTasks remoteTasks;
    TaskConfigurationDiscoveryProcessor taskConfigurationDiscoveryProcessor;

    @BeforeEach
    void init() {
        tasks.clear();
        taskConfigurationDiscoveryProcessor = new TaskConfigurationDiscoveryProcessor(
            properties,
            distributedTaskService,
            distributedTaskPropertiesMapper,
            distributedTaskPropertiesMerger,
            tasks,
            remoteTasks,
            EMPTY_TASK_SETTINGS_CUSTOMIZER
        );
    }

    @AfterEach
    void teardown() throws InterruptedException {
        if (taskConfigurationDiscoveryProcessor != null) {
            taskConfigurationDiscoveryProcessor.shutdown();
        }
    }

    @Test
    void shouldRegistryLocalTasksWithDefaultConfigFromCode() {
        //when
        DefaultTask defaultTask = new DefaultTask();
        tasks.add(defaultTask);

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        verifyTaskIsRegistered(defaultTask, TaskSettings.DEFAULT);
    }

    @SneakyThrows
    @Test
    void shouldRegistryLocalTasksWithDefaultConfigFromFile() {
        //when
        when(properties.getTaskPropertiesGroup()).thenReturn(buildDefaultConfig());
        DefaultTask defaultTask = new DefaultTask();
        tasks.add(defaultTask);

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder()
            .maxParallelInCluster(200)
            .cron("*/2 * * * * *")
            .retry(Retry.builder()
                .retryMode(RetryMode.BACKOFF)
                .fixed(TaskSettings.DEFAULT.getRetry().getFixed())
                .backoff(Backoff.builder()
                    .delayPeriod(Duration.ofDays(1))
                    .build())
                .build())
            .build();

        verifyTaskIsRegistered(defaultTask, taskSettings);
        verifyCronTaskIsScheduled(defaultTask);
    }

    @Test
    void shouldRegistryLocalTaskWithSimpleCronCustomConfigFromCodeAndFromCustomInFile() {
        //when
        SimpleCronCustomizedTask customizedTask = new SimpleCronCustomizedTask();
        tasks.add(customizedTask);
        when(properties.getTaskPropertiesGroup()).thenReturn(DistributedTaskProperties.TaskPropertiesGroup.builder()
            .taskProperties(Map.of(
                customizedTask.getDef().getTaskName(),
                DistributedTaskProperties.TaskProperties.builder()
                    .cron("* */20 * * *")
                    .build()
            ))
            .build()
        );

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder()
            .maxParallelInCluster(1)
            .cron("* */20 * * *")
            .build();
        verifyTaskIsRegistered(customizedTask, taskSettings);
        verifyCronTaskIsScheduled(customizedTask);
    }

    @Test
    void shouldRegistryLocalTaskWithCustomTaskWithRetryOffFromCodeAndFromCustomInFile() {
        //when
        CustomTaskWithOffRetry customizedTask = new CustomTaskWithOffRetry();
        tasks.add(customizedTask);
        when(properties.getTaskPropertiesGroup()).thenReturn(DistributedTaskProperties.TaskPropertiesGroup.builder()
            .taskProperties(Map.of(
                customizedTask.getDef().getTaskName(),
                DistributedTaskProperties.TaskProperties.builder()
                    .maxParallelInCluster(15)
                    .build()
            ))
            .build()
        );

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder()
            .maxParallelInCluster(15)
            .retry(TaskSettings.DEFAULT.getRetry().toBuilder()
                .retryMode(RetryMode.OFF)
                .build())
            .build();
        verifyTaskIsRegistered(customizedTask, taskSettings);
    }

    @Test
    void shouldRegistryLocalTaskWithCustomConfigFromCode() {
        //when
        CustomizedTask customizedTask = new CustomizedTask();
        tasks.add(customizedTask);

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder()
            .maxParallelInCluster(10)
            .cron("* */10 * * *")
            .executionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
            .dltEnabled(true)
            .retry(Retry.builder()
                .retryMode(RetryMode.BACKOFF)
                .fixed(TaskSettings.DEFAULT.getRetry().getFixed())
                .backoff(Backoff.builder()
                    .initialDelay(Duration.ofMinutes(1))
                    .delayPeriod(Duration.ofSeconds(10))
                    .maxRetries(100)
                    .maxDelay(Duration.ofHours(1))
                    .build())
                .build())
            .build();

        verifyTaskIsRegistered(customizedTask, taskSettings);
        verifyCronTaskIsScheduled(customizedTask);
    }

    @Test
    void shouldRegistryLocalTaskWithCustomConfigFromCodeAndOverrideDefaultConfigFromFile() {
        //when
        when(properties.getTaskPropertiesGroup()).thenReturn(buildDefaultConfig());
        CustomizedTask customizedTask = new CustomizedTask();
        tasks.add(customizedTask);

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder()
            .maxParallelInCluster(10)
            .cron("* */10 * * *")
            .executionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
            .dltEnabled(true)
            .retry(Retry.builder()
                .retryMode(RetryMode.BACKOFF)
                .fixed(TaskSettings.DEFAULT.getRetry().getFixed())
                .backoff(Backoff.builder()
                    .initialDelay(Duration.ofMinutes(1))
                    .delayPeriod(Duration.ofDays(1))
                    .maxRetries(100)
                    .build())
                .build())
            .build();

        verifyTaskIsRegistered(customizedTask, taskSettings);
        verifyCronTaskIsScheduled(customizedTask);
    }

    @Test
    void shouldRegistryLocalTaskWithCustomConfigFromFile() {
        //when
        when(properties.getTaskPropertiesGroup()).thenReturn(buildCustomConfig());
        CustomizedTask customizedTask = new CustomizedTask();
        tasks.add(customizedTask);

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder()
            .maxParallelInCluster(300)
            .cron("*/4 * * * *")
            .executionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
            .dltEnabled(true)
            .retry(Retry.builder()
                .retryMode(RetryMode.FIXED)
                .fixed(Fixed.builder()
                    .delay(Duration.ofSeconds(10))
                    .maxNumber(6)
                    .maxInterval(Duration.ofHours(2))
                    .build())
                .backoff(Backoff.builder()
                    .initialDelay(Duration.ofMinutes(1))
                    .delayPeriod(Duration.ofDays(1))
                    .maxRetries(100)
                    .build())
                .build())
            .build();

        verifyTaskIsRegistered(customizedTask, taskSettings);
        verifyCronTaskIsScheduled(customizedTask);
    }

    @Test
    void shouldRegisterRemoteTask() {
        //when
        TaskSettings taskSettings = TaskSettings.DEFAULT.toBuilder().build();
        TaskDef<String> remoteTaskDef = TaskDef.publicTaskDef(
            "remote-app",
            "remote",
            String.class
        );
        when(remoteTasks.remoteTasks()).thenReturn(List.of(remoteTaskDef));

        //do
        taskConfigurationDiscoveryProcessor.init();

        //verify
        verify(distributedTaskService)
            .registerRemoteTask(eq(remoteTaskDef), eq(taskSettings));
    }

    private DistributedTaskProperties.TaskPropertiesGroup buildCustomConfig() {
        return DistributedTaskProperties.TaskPropertiesGroup.builder()
            .defaultProperties(buildDefaultTaskSettings())
            .taskProperties(Map.of(
                "customized",
                DistributedTaskProperties.TaskProperties.builder()
                    .maxParallelInCluster(300)
                    .cron("*/4 * * * *")
                    .retry(DistributedTaskProperties.Retry.builder()
                        .retryMode(RetryMode.FIXED.toString())
                        .fixed(DistributedTaskProperties.Fixed.builder()
                            .maxInterval(Duration.ofHours(2))
                            .build())
                        .build())
                    .build()
            ))
            .build();
    }

    private DistributedTaskProperties.TaskPropertiesGroup buildDefaultConfig() {
        return DistributedTaskProperties.TaskPropertiesGroup.builder()
            .defaultProperties(buildDefaultTaskSettings())
            .build();
    }

    private DistributedTaskProperties.TaskProperties buildDefaultTaskSettings() {
        return DistributedTaskProperties.TaskProperties.builder()
            .maxParallelInCluster(200)
            .cron("*/2 * * * * *")
            .retry(DistributedTaskProperties.Retry.builder()
                .retryMode(RetryMode.BACKOFF.toString())
                .backoff(DistributedTaskProperties.Backoff.builder()
                    .delayPeriod(Duration.ofDays(1))
                    .build())
                .build())
            .build();
    }

    private void verifyTaskIsRegistered(Task<?> task, TaskSettings taskSettings) {
        verify(distributedTaskService).registerTask(
            argThat(t -> task.getClass().equals(t.getClass())),
            argThat(taskSettings::equals)
        );
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    private void verifyCronTaskIsScheduled(Task<?> cronTask) {
        verify(distributedTaskService, timeout(10_000)).schedule(
            argThat(o -> Objects.equals(o, cronTask.getDef())),
            any(ExecutionContext.class)
        );
    }
}
