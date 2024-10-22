package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedLocalRescheduleTest extends BaseLocalWorkerIntegrationTest {

    @ParameterizedTest
    @EnumSource(ActionMode.class)
    void shouldRescheduleCurrentTaskWhenFromTask(ActionMode actionMode) {
        //when
        setFixedTime();
        var parentTestTaskModel = buildActionAndGenerateTask(
            m -> {
                distributedTaskService.reschedule(m.getCurrentTaskId(), Duration.ofSeconds(10));
                assertThat(taskRepository.find(m.getCurrentTaskId().getId())).isPresent()
                    .get()
                    .matches(te -> te.getVersion() == 1, "opt locking");
            },
            String.class,
            actionMode
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertThat(taskRepository.find(parentTestTaskModel.getTaskId().getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 10L, "reschedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @Test
    void shouldWinOnFailureWhenRescheduleCurrentTaskWhenFromTask() {
        //when
        long shiftFromAction = 33;
        long shiftFromFailure = 44;

        setFixedTime();
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofSeconds(shiftFromAction));
                throw new RuntimeException();
            })
            .failureAction(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofSeconds(shiftFromFailure));
                return true;
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        assertThat(taskRepository.find(parentTestTaskModel.getTaskId().getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == shiftFromFailure, "reschedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @Test
    void shouldNotRescheduleCurrentTaskWhenNotLastOnFailure() {
        //when
        long shiftFromAction = 33;
        long shiftFromFailure = 44;

        setFixedTime();
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofSeconds(shiftFromAction));
                throw new RuntimeException();
            })
            .failureAction(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofSeconds(shiftFromFailure));
                return false;
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        long nextAttempt = parentTestTaskModel.getTaskSettings().getRetry().getFixed().getDelay().toSeconds();
        assertThat(taskRepository.find(parentTestTaskModel.getTaskId().getId())).isPresent()
            .get()
            .matches(te -> te.getFailures() == 1, "failures")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == nextAttempt, "reschedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @Test
    void shouldNotRescheduleCurrentTaskWhenLastOnFailureAndException() {
        //when
        long shiftFromAction = 33;
        long shiftFromFailure = 44;

        setFixedTime();
        var parentTestTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .taskEntityCustomizer(extendedTaskGenerator.withLastAttempt())
            .action(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofSeconds(shiftFromAction));
                throw new RuntimeException();
            })
            .failureAction(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofSeconds(shiftFromFailure));
                throw new RuntimeException();
            })
            .build()
        );

        //do
        getTaskWorker().execute(parentTestTaskModel.getTaskEntity(), parentTestTaskModel.getRegisteredTask());

        //verify
        verifyLocalTaskIsFinished(parentTestTaskModel.getTaskEntity());
    }

    //todo: all test below should be rewritten like for both ActionMode

    @Test
    void shouldNotRescheduleImmediatelyCurrentTaskWhenFromTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleImmediately(m.getCurrentTaskId(), Duration.ofSeconds(10));
            assertThat(taskRepository.find(m.getCurrentTaskId().getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 1, "opt locking");
        });

        setFixedTime();
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        assertThat(taskRepository.find(taskId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 10L, "reschedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @Test
    void shouldRescheduleOtherTaskWhenFromTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(otherTakId, Duration.ofSeconds(10));
            assertThat(taskRepository.find(otherTakId.getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 1, "opt locking");
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 10L, "reschedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @Test
    void shouldRescheduleImmediatelyOtherTaskWhenFromTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleImmediately(otherTakId, Duration.ofSeconds(10));
            assertThat(taskRepository.find(otherTakId.getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 2, "opt locking");
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 10L, "reschedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    @SneakyThrows
    @Test
    void shouldNotRescheduleTaskWhenError() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(otherTakId, Duration.ofSeconds(10));
            distributedTaskService.reschedule(m.getCurrentTaskId(), Duration.ofSeconds(20));
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
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
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 1, "opt locking");
    }

    @SneakyThrows
    @Test
    void shouldRescheduleOtherTaskAndNotCurrentWhenErrorAndImmediately() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleImmediately(otherTakId, Duration.ofSeconds(10));
            distributedTaskService.rescheduleImmediately(m.getCurrentTaskId(), Duration.ofSeconds(20));
            throw new RuntimeException();
        });
        mockedTask = Mockito.spy(mockedTask);

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
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
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 10L, "next retry time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker");
    }

    @SneakyThrows
    @Test
    void shouldNotRescheduleTaskWhenParallelExecution() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(otherTakId, Duration.ofSeconds(20));
            distributedTaskService.reschedule(m.getCurrentTaskId(), Duration.ofSeconds(20));
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();
        UUID foreignWorkerId = emulateParallelExecution(taskEntity);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyParallelExecution(taskEntity, foreignWorkerId);
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 1, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "next retry time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker");
    }

    @SneakyThrows
    @Test
    void shouldRescheduleOtherTaskWhenParallelExecutionAndImmediately() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.rescheduleImmediately(otherTakId, Duration.ofSeconds(20));
            distributedTaskService.reschedule(m.getCurrentTaskId(), Duration.ofSeconds(20));
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();
        UUID foreignWorkerId = emulateParallelExecution(taskEntity);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyParallelExecution(taskEntity, foreignWorkerId);
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 20L, "next retry time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker");
    }

    @SneakyThrows
    @Test
    void shouldRescheduleOtherTaskWhenParallelExecution() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(otherTakId, Duration.ofSeconds(20));
        });

        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        TaskEntity taskEntity = saveNewTaskEntity();
        emulateParallelExecution(otherTaskEntity);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 3, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 20L, "next retry time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker");
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldPostponeCronTask() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder()
            .cron("*/50 * * * * *")
            .build();
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(
                m.getCurrentTaskId(),
                Duration.ofHours(1)
            );
        });
        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verify(mockedTask, Mockito.never()).onFailure(any(FailedExecutionContext.class));
        assertThat(taskRepository.find(taskEntity.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 3600L, "next execution time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }
}
