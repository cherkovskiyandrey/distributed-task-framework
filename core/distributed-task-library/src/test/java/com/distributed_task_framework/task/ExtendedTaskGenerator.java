package com.distributed_task_framework.task;

import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.StateHolder;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.model.TypeDef;
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
import org.mockito.Mockito;
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

    public <T> TestTaskModel<T> generateDefaultAndSave(Class<T> inputType, String taskName) {
        return generate(TestTaskModelSpec.builder(inputType)
            .withSaveInstance()
            .privateTask(taskName)
            .build()
        );
    }

    public <T> TestTaskModel<T> generateJoinByNameAndSave(Class<T> inputType, String taskName) {
        return generate(TestTaskModelSpec.builder(inputType)
            .withSaveInstance()
            .taskEntityCustomizer(AbstractTaskModelSpec.JOIN_TASK)
            .privateTask(taskName)
            .build()
        );
    }

    public <T> TestTaskModel<T> generate(TestTaskModelSpec<T> taskModelSpec) {
        var baseTestTaskModel = generateBase(taskModelSpec);

        Task<T> mockedTask = createMockedTask(baseTestTaskModel.getTaskDef(), taskModelSpec);
        RegisteredTask<T> registeredTask = registryMockedTask(
            baseTestTaskModel.getTaskDef(),
            baseTestTaskModel.getTaskSettings(),
            mockedTask
        );

        return baseTestTaskModel.toBuilder()
            .mockedTask(mockedTask)
            .registeredTask(registeredTask)
            .build();
    }

    public <T, U> TestStatefulTaskModel<T, U> generate(TestStatefulTaskModelSpec<T, U> taskModelSpec) {
        var stateDef = createTypeDef(taskModelSpec);
        var baseTestTaskModel = generateBase(taskModelSpec);


        Task<T> mockedTask = createMockedTask(baseTestTaskModel.getTaskDef(), stateDef, taskModelSpec);
        RegisteredTask<T> registeredTask = registryMockedTask(
            baseTestTaskModel.getTaskDef(),
            baseTestTaskModel.getTaskSettings(),
            mockedTask
        );

        return TestStatefulTaskModel.<T, U>buildStateful()
            .taskDef(baseTestTaskModel.getTaskDef())
            .stateDef(stateDef)
            .taskSettings(baseTestTaskModel.getTaskSettings())
            .taskEntity(baseTestTaskModel.getTaskEntity())
            .taskId(baseTestTaskModel.getTaskId())
            .mockedTask(mockedTask)
            .registeredTask(registeredTask)
            .build();
    }

    private <T> TestTaskModel<T> generateBase(AbstractTaskModelSpec<T> abstractTaskModelSpec) {
        TaskDef<T> taskDef = createTaskDef(abstractTaskModelSpec);
        TaskSettings taskSettings = createTaskSettings(abstractTaskModelSpec);

        TaskEntity taskEntity = createTaskEntity(taskDef, abstractTaskModelSpec);
        TaskId taskId = createTaskId(taskEntity);

        return TestTaskModel.<T>builder()
            .taskDef(taskDef)
            .taskSettings(taskSettings)
            .taskEntity(taskEntity)
            .taskId(taskId)
            .build();
    }

    @Nullable
    private TaskId createTaskId(@Nullable TaskEntity taskEntity) {
        return taskEntity != null ? taskMapper.map(taskEntity, commonSettings.getAppName()) : null;
    }

    private <T> RegisteredTask<T> registryMockedTask(TaskDef<T> taskDef,
                                                     TaskSettings taskSettings,
                                                     Task<T> mockedTask) {
        RegisteredTask<T> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<T>getRegisteredLocalTask(eq(taskDef.getTaskName()))).thenReturn(Optional.of(registeredTask));
        return registeredTask;
    }

    private <T> Task<T> createMockedTask(TaskDef<T> taskDef, TestTaskModelSpec<T> testTaskModelSpec) {
        return Mockito.spy(TaskGenerator.defineTask(
                taskDef,
                createAction(testTaskModelSpec),
                createFailedAction(testTaskModelSpec)
            )
        );
    }

    private <T> TaskGenerator.Consumer<ExecutionContext<T>> createAction(TestTaskModelSpec<T> testTaskModelSpec) {
        return testTaskModelSpec.getAction() != null ? testTaskModelSpec.getAction() : ctx -> {
        };
    }

    private <T> TaskGenerator.Function<FailedExecutionContext<T>, Boolean> createFailedAction(TestTaskModelSpec<T> testTaskModelSpec) {
        return testTaskModelSpec.getFailureAction() != null ? testTaskModelSpec.getFailureAction() : ctx -> false;
    }

    private <T, U> Task<T> createMockedTask(TaskDef<T> taskDef,
                                            TypeDef<U> stateDef,
                                            TestStatefulTaskModelSpec<T, U> taskModelSpec) {
        return Mockito.spy(TaskGenerator.defineTask(
                taskDef,
                stateDef,
                createAction(taskModelSpec),
                createFailedAction(taskModelSpec)
            )
        );
    }

    private <T, U> TaskGenerator.BiFunction<FailedExecutionContext<T>, StateHolder<U>, Boolean> createFailedAction(TestStatefulTaskModelSpec<T, U> taskModelSpec) {
        return taskModelSpec.getFailureAction() != null ? taskModelSpec.getFailureAction() : (ctx, holder) -> false;
    }

    private <T, U> TaskGenerator.BiConsumer<ExecutionContext<T>, StateHolder<U>> createAction(TestStatefulTaskModelSpec<T, U> taskModelSpec) {
        return taskModelSpec.getAction() != null ? taskModelSpec.getAction() : (ctx, holder) -> {
        };
    }

    private <T> TaskDef<T> createTaskDef(AbstractTaskModelSpec<T> baseTaskDef) {
        return baseTaskDef.getTaskDef() != null ?
            baseTaskDef.getTaskDef() :
            TaskDef.privateTaskDef("test-" + RandomStringUtils.random(10), baseTaskDef.getInputType());
    }


    private <U, T> TypeDef<U> createTypeDef(TestStatefulTaskModelSpec<T, U> taskModelSpec) {
        return taskModelSpec.getStateDef() != null ?
            taskModelSpec.getStateDef() :
            TypeDef.of(taskModelSpec.getStateType());
    }

    private <T> TaskSettings createTaskSettings(AbstractTaskModelSpec<T> abstractTaskModelSpec) {
        TaskSettings taskSettings = abstractTaskModelSpec.isRecurrent() ?
            defaultTaskSettings.toBuilder().cron("*/50 * * * * *").build() :
            defaultTaskSettings.toBuilder().build();

        return abstractTaskModelSpec.getTaskSettingsCustomizer() != null ?
            abstractTaskModelSpec.getTaskSettingsCustomizer().apply(taskSettings) :
            taskSettings;
    }

    @SneakyThrows
    @Nullable
    private <T> TaskEntity createTaskEntity(TaskDef<T> taskDef, AbstractTaskModelSpec<T> abstractTaskModelSpec) {
        if (!abstractTaskModelSpec.isSaveInstance()) {
            return null;
        }

        TaskEntity taskEntity = TaskEntity.builder()
            .taskName(taskDef.getTaskName())
            .id(UUID.randomUUID())
            .workflowId(abstractTaskModelSpec.getWorkflowId() != null ? abstractTaskModelSpec.getWorkflowId() : UUID.randomUUID())
            .virtualQueue(VirtualQueue.NEW)
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .createdDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .messageBytes(taskSerializer.writeValue("hello message"))
            .localState(serializeLocalStateIfExists(abstractTaskModelSpec))
            .failures(0)
            .notToPlan(false)
            .build();

        taskEntity = abstractTaskModelSpec.getTaskEntityCustomizer() != null ?
            abstractTaskModelSpec.getTaskEntityCustomizer().apply(taskEntity) :
            taskEntity;
        return taskRepository.saveOrUpdate(taskEntity);
    }

    @Nullable
    @SneakyThrows
    private <T> byte[] serializeLocalStateIfExists(AbstractTaskModelSpec<T> abstractTaskModelSpec) {
        if (abstractTaskModelSpec instanceof TestStatefulTaskModelSpec<?, ?> statefulTaskModelSpec) {
            if (statefulTaskModelSpec.getLocalState() != null) {
                return taskSerializer.writeValue(statefulTaskModelSpec.getLocalState());
            }
        }
        return null;
    }
}
