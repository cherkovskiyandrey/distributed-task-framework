package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.entity.TestBusinessObjectEntity;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.RetryMode;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import com.distributed_task_framework.task.TestTaskModelSpec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.jackson.Jacksonized;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractLocalWorkerIntegrationTests extends BaseLocalWorkerIntegrationTest {
    @Autowired
    ObjectMapper objectMapper;

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldExecuteWhenSimpleTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = (Task<String>) Mockito.mock(Task.class);
        when(mockedTask.getDef()).thenReturn(taskDef);

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verify(mockedTask).execute(argThat(argument ->
                taskId.equals(argument.getCurrentTaskId()) &&
                    taskEntity.getWorkflowId().equals(argument.getWorkflowId()) &&
                    "hello message".equals(argument.getInputMessageOrThrow())
            )
        );
        verifyTaskIsFinished(taskId);
    }

    @Test
    void shouldNotScheduleNewTaskButSaveBusinessObjectWhenExceptionInFailure() {
        //when
        setFixedTime();
        var childTestTaskModel = extendedTaskGenerator.generateDefault(String.class);
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(TestTaskModelSpec.throwException())
            .failureAction(ctx -> {
                distributedTaskService.schedule(childTestTaskModel.getTaskDef(), ExecutionContext.empty());
                testBusinessObjectRepository.save(TestBusinessObjectEntity.builder().build());
                throw new RuntimeException();
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyTaskInNextAttempt(parentTestTaskModel.getTaskId(), parentTestTaskModel.getTaskSettings());
        assertThat(testBusinessObjectRepository.findAll()).hasSize(1);
    }

    @SneakyThrows
    @Test
    void shouldBeTolerantWhenDtoIsExpanded() {
        //when
        TaskDef<MessageDto> taskDef = TaskDef.privateTaskDef("test", new TypeReference<>() {
        });
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<MessageDto> mockedTask = TaskGenerator.emptyDefineTask(taskDef);
        mockedTask = Mockito.spy(mockedTask);

        RegisteredTask<MessageDto> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        String serializedValueAsStr = """
            {
               "text": "hello_world!",
               "checkpoints": [1, 2, 3],
               "internalEnum": "NEW_VALUE",
               "newStringField": "value_for_new_field",
               "newIntValue": 123,
               "newSubObjectField": {
                    "newSubObjectStringField": "value_for_new_sub_object_field",
                    "newSubObjectIntValue": 123
               }
            }
            """
            .replaceAll("\\s", "")
            .replaceAll("\\v", "");
        byte[] valueAsBytes = serializedValueAsStr.getBytes(StandardCharsets.UTF_8);
        TaskEntity taskEntity = taskRepository.saveOrUpdate(TaskEntity.builder()
            .taskName("test")
            .id(UUID.randomUUID())
            .workflowId(UUID.randomUUID())
            .virtualQueue(VirtualQueue.NEW)
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .createdDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .messageBytes(valueAsBytes)
            .build()
        );

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        MessageDto expectedMessageDto = MessageDto.builder()
            .text("hello_world!")
            .checkpoint(1)
            .checkpoint(2)
            .checkpoint(3)
            .build();
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verify(mockedTask).execute(argThat(argument ->
                taskId.equals(argument.getCurrentTaskId()) &&
                    taskEntity.getWorkflowId().equals(argument.getWorkflowId()) &&
                    expectedMessageDto.equals(argument.getInputMessageOrThrow())
            )
        );
        verifyLocalTaskIsFinished(taskEntity);

    }

    /**
     * @noinspection unchecked
     */
    @SneakyThrows
    @Test
    void shouldExecuteParameterizedTypeTreadSafely() {
        //when
        TaskDef<List<String>> taskDef = TaskDef.privateTaskDef("test", new TypeReference<>() {
        });
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<List<String>> mockedTask = TaskGenerator.emptyDefineTask(taskDef);
        mockedTask = Mockito.spy(mockedTask);

        RegisteredTask<List<String>> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = taskRepository.saveOrUpdate(TaskEntity.builder()
            .taskName("test")
            .id(UUID.randomUUID())
            .workflowId(UUID.randomUUID())
            .virtualQueue(VirtualQueue.NEW)
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .createdDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .messageBytes(taskSerializer.writeValue(List.of("hello", "world")))
            .build()
        );

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verify(mockedTask).execute(argThat(argument ->
                taskId.equals(argument.getCurrentTaskId()) &&
                    taskEntity.getWorkflowId().equals(argument.getWorkflowId()) &&
                    List.of("hello", "world").equals(argument.getInputMessageOrThrow())
            )
        );
        verifyLocalTaskIsFinished(taskEntity);
    }

    /**
     * @noinspection unchecked
     */
    @SneakyThrows
    @Test
    void shouldExecuteParameterizedTypeAsComplexObjectThreadSafely() {
        //when
        TaskDef<MessageDto> taskDef = TaskDef.privateTaskDef("test", new TypeReference<>() {
        });
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<MessageDto> mockedTask = TaskGenerator.emptyDefineTask(taskDef);
        mockedTask = Mockito.spy(mockedTask);

        RegisteredTask<MessageDto> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        MessageDto messageDto = MessageDto.builder()
            .text("hello world!")
            .checkpoint(1)
            .checkpoint(2)
            .checkpoint(2)
            .build();
        TaskEntity taskEntity = taskRepository.saveOrUpdate(TaskEntity.builder()
            .taskName("test")
            .id(UUID.randomUUID())
            .workflowId(UUID.randomUUID())
            .virtualQueue(VirtualQueue.NEW)
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .createdDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .messageBytes(taskSerializer.writeValue(messageDto))
            .build()
        );

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verify(mockedTask).execute(argThat(argument ->
                taskId.equals(argument.getCurrentTaskId()) &&
                    taskEntity.getWorkflowId().equals(argument.getWorkflowId()) &&
                    messageDto.equals(argument.getInputMessageOrThrow())
            )
        );
        verifyLocalTaskIsFinished(taskEntity);
    }

    @Test
    void shouldPostponeWhenExceptionAndRetryPolicy() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verifyFirstAttemptTaskOnFailure(mockedTask, taskEntity);

        assertThat(taskRepository.find(taskId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 10L, "next retry time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @Test
    void shouldNotPostponeWhenExceptionAndInterruptRetryingAndNotLastError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(
            taskDef,
            m -> {
                throw new RuntimeException();
            },
            TaskGenerator.interruptedRetry()
        );
        mockedTask = Mockito.spy(mockedTask);

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
    }

    @Test
    void shouldMoveToDltWhenLastErrorAndDltEnabled() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder()
            .dltEnabled(true)
            .build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity(5);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verifyLastAttemptTaskOnFailure(mockedTask, taskEntity);

        verifyLocalTaskIsFinished(taskEntity);
        assertThat(dltRepository.findById(taskId.getId())).isPresent()
            .get()
            .matches(te -> taskEntity.getTaskName().equals(te.getTaskName()), "task name")
            .matches(te -> taskEntity.getWorkflowId().equals(te.getWorkflowId()), "workflowId")
            .matches(te -> te.getFailures() == 6, "failures")
            .matches(te -> {
                try {
                    return "hello message".equals(taskSerializer.readValue(te.getMessageBytes(), String.class));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, "input message");
    }

    @Test
    void shouldSetAttemptNumberInExecutionContext() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder()
            .build();
        AtomicInteger receivedAttempts = new AtomicInteger(0);
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            receivedAttempts.set(m.getExecutionAttempt());
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity(5);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        assertThat(receivedAttempts.get()).isEqualTo(6);

        verifyLastAttemptTaskOnFailure(mockedTask, taskEntity);
    }

    /**
     * @noinspection unchecked
     */
    @SneakyThrows
    @Test
    void shouldExecuteWhenCronTask() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();
        Task<Void> mockedTask = (Task<Void>) Mockito.mock(Task.class);
        when(mockedTask.getDef()).thenReturn(taskDef);
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();
        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verify(mockedTask).execute(argThat(argument ->
                taskId.equals(argument.getCurrentTaskId()) &&
                    taskEntity.getWorkflowId().equals(argument.getWorkflowId())
            )
        );
        verifyCronTaskInRepositoryToNextCall(taskEntity);
    }

    @Test
    void shouldPostponeCronTaskWhenExceptionAndRetryPolicy() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyFirstAttemptCronTaskOnFailure(mockedTask, taskEntity);
        assertThat(taskRepository.find(taskEntity.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 10L, "next execution time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @Test
    void shouldRescheduleCronTaskWhenLastFailure() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity(5);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLastAttemptCronTaskOnFailure(mockedTask, taskEntity);
        verifyCronTaskInRepositoryToNextCall(taskEntity);
    }

    @Test
    void shouldRescheduleCronTaskWhenExceptionAndInterruptRetryingAndNotLastError() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();
        Task<Void> mockedTask = TaskGenerator.defineTask(
            taskDef,
            m -> {
                throw new RuntimeException();
            },
            TaskGenerator.interruptedRetry()
        );
        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyCronTaskInRepositoryToNextCall(taskEntity);
    }

    @Test
    void shouldRescheduleCronTaskWhenFirstFailureAndRetryOff() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings().toBuilder()
            .retry(Retry.builder()
                .retryMode(RetryMode.OFF)
                .build())
            .build();
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLastAttemptCronTaskOnFailure(mockedTask, taskEntity, 1);
        verifyCronTaskInRepositoryToNextCall(taskEntity);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldNotExecuteAndFinalizeCanceledTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = (Task<String>) Mockito.mock(Task.class);
        when(mockedTask.getDef()).thenReturn(taskDef);

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();
        taskEntity = taskEntity.toBuilder()
            .canceled(true)
            .build();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verify(mockedTask, never()).execute(any(ExecutionContext.class));
        verifyLocalTaskIsFinished(taskEntity);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldReExecuteTaskWhenAttemptsMoreThanOnce() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = (Task<String>) Mockito.mock(Task.class);
        when(mockedTask.getDef()).thenReturn(taskDef);

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity(1);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verify(mockedTask).reExecute(any(ExecutionContext.class));
        verifyLocalTaskIsFinished(taskEntity);
    }

    @Value
    @Builder
    @Jacksonized
    public static class MessageDto {
        String text;
        @Singular
        Set<Integer> checkpoints;
    }
}
