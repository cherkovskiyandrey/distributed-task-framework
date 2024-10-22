package com.distributed_task_framework.task;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@FieldDefaults(level = AccessLevel.PRIVATE)
public class ExtendedTaskGenerator {
    //because we use SpyBean
    private final TaskRegistryService taskRegistryService;
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    TaskSerializer taskSerializer;
    @Autowired
    TaskMapper taskMapper;
    @Getter
    @Autowired
    TaskSettings defaultTaskSettings;
    @Autowired
    CommonSettings commonSettings;
    @Autowired
    Clock clock;

    public ExtendedTaskGenerator(TaskRegistryService taskRegistryService) {
        this.taskRegistryService = taskRegistryService;
    }

    public Function<TaskEntity, TaskEntity> withLastAttempt() {
        return
            taskEntity -> taskEntity.toBuilder()
                .failures(getDefaultTaskSettings().getRetry().getFixed().getMaxNumber() + 1)
                .build();
    }

    public <T> TestTaskModel<T> generateDefault(Class<T> inputType) {
        return generate(TestTaskModelSpec.builder(inputType).build());
    }

    public <T> TestTaskModel<T> generateDefaultAndSave(Class<T> inputType) {
        return generate(TestTaskModelSpec.builder(inputType).withSaveInstance().build());
    }

    public <T> TestTaskModel<T> generateJoinByNameAndSave(Class<T> inputType, String name) {
        return generate(TestTaskModelSpec.builder(inputType)
            .withSaveInstance()
            .taskEntityCustomizer(TestTaskModelSpec.JOIN_TASK)
            .privateTask(name)
            .build()
        );
    }

    public <T> TestTaskModel<T> generate(TestTaskModelSpec<T> testTaskModelSpec) {
        TaskDef<T> taskDef = createTaskDef(testTaskModelSpec);
        TaskSettings taskSettings = createTaskSettings(testTaskModelSpec);

        TaskEntity taskEntity = createTaskEntity(taskDef, testTaskModelSpec);
        TaskId taskId = createTaskId(taskEntity);

        Task<T> mockedTask = createMockedTask(taskDef, testTaskModelSpec);
        RegisteredTask<T> registeredTask = registryMockedTask(taskDef, taskSettings, mockedTask);

        return TestTaskModel.<T>builder().taskDef(taskDef).taskSettings(taskSettings).taskEntity(taskEntity).taskId(taskId).registeredTask(registeredTask).build();
    }

    @Nullable
    private TaskId createTaskId(@Nullable TaskEntity taskEntity) {
        return taskEntity != null ? taskMapper.map(taskEntity, commonSettings.getAppName()) : null;
    }

    private <T> RegisteredTask<T> registryMockedTask(TaskDef<T> taskDef, TaskSettings taskSettings, Task<T> mockedTask) {
        RegisteredTask<T> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<T>getRegisteredLocalTask(eq(taskDef.getTaskName()))).thenReturn(Optional.of(registeredTask));
        return registeredTask;
    }

    private <T> Task<T> createMockedTask(TaskDef<T> taskDef, TestTaskModelSpec<T> testTaskModelSpec) {
        return TaskGenerator.defineTask(taskDef, createAction(testTaskModelSpec), createFailedAction(testTaskModelSpec));
    }

    private <T> TaskGenerator.Consumer<ExecutionContext<T>> createAction(TestTaskModelSpec<T> testTaskModelSpec) {
        return testTaskModelSpec.getAction() != null ? testTaskModelSpec.getAction() : ctx -> {
        };
    }

    private <T> TaskGenerator.Function<FailedExecutionContext<T>, Boolean> createFailedAction(TestTaskModelSpec<T> testTaskModelSpec) {
        return testTaskModelSpec.getFailureAction() != null ? testTaskModelSpec.getFailureAction() : ctx -> false;
    }

    private <T> TaskDef<T> createTaskDef(TestTaskModelSpec<T> baseTaskDef) {
        return baseTaskDef.getTaskDef() != null ? baseTaskDef.getTaskDef() : TaskDef.privateTaskDef("test-" + RandomStringUtils.random(10), baseTaskDef.getInputType());
    }

    private <T> TaskSettings createTaskSettings(TestTaskModelSpec<T> testTaskModelSpec) {
        TaskSettings taskSettings = testTaskModelSpec.isRecurrent() ? defaultTaskSettings.toBuilder().cron("*/50 * * * * *").build() : defaultTaskSettings.toBuilder().build();

        return testTaskModelSpec.getTaskSettingsCustomizer() != null ? testTaskModelSpec.getTaskSettingsCustomizer().apply(taskSettings) : taskSettings;
    }

    @SneakyThrows
    @Nullable
    private <T> TaskEntity createTaskEntity(TaskDef<T> taskDef, TestTaskModelSpec<T> testTaskModelSpec) {
        if (!testTaskModelSpec.isSaveInstance()) {
            return null;
        }

        TaskEntity taskEntity = TaskEntity.builder()
            .taskName(taskDef.getTaskName())
            .id(UUID.randomUUID())
            .workflowId(UUID.randomUUID())
            .virtualQueue(VirtualQueue.NEW)
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .createdDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .messageBytes(taskSerializer.writeValue("hello message"))
            .failures(0)
            .notToPlan(false)
            .build();

        taskEntity = testTaskModelSpec.getTaskEntityCustomizer() != null ?
            testTaskModelSpec.getTaskEntityCustomizer().apply(taskEntity) :
            taskEntity;
        return taskRepository.saveOrUpdate(taskEntity);
    }
}
