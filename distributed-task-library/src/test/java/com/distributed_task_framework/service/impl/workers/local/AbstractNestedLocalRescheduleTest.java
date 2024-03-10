package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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

    @Test
    void shouldRescheduleCurrentTaskWhenFromTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(m.getCurrentTaskId(), Duration.ofSeconds(10));
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
