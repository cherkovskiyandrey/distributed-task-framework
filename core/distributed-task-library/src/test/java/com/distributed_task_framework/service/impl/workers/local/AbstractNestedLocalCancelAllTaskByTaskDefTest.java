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
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedLocalCancelAllTaskByTaskDefTest extends BaseLocalWorkerIntegrationTest {

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldCancelAllTaskByTaskDef() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDef(taskDef);
        });

        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        TaskEntity taskEntity = saveNewTaskEntity();
        TaskEntity otherTaskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verify(mockedTask, Mockito.never()).onFailure(any(FailedExecutionContext.class));
        verifyLocalTaskIsFinished(taskEntity);
        verifyTaskIsCanceled(otherTaskEntity);
    }

    @Test
    void shouldNotCancelAllTaskByTaskDefWhenParallelExecution() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDef(taskDef);
        });

        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        UUID foreignWorkerId = emulateParallelExecution(taskEntity);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyParallelExecution(taskEntity, foreignWorkerId);
        assertThat(taskRepository.find(otherTaskEntity.getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 1, "version")
                .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "execution time")
                .matches(te -> !foreignWorkerId.equals(te.getAssignedWorker()), "foreign assigned worker")
        ;
    }

    @Test
    void shouldCancelAllTaskByTaskDefOtherTaskAndNotCurrentWhenParallelExecutionAndImmediately() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDefImmediately(taskDef);
            verifyTaskIsCanceled(otherTaskEntity);
        });

        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        UUID foreignWorkerId = emulateParallelExecution(taskEntity);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyParallelExecution(taskEntity, foreignWorkerId);
        verifyTaskIsCanceled(otherTaskEntity);
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldCancelAllTaskByTaskDefWhenParallelExecutionOfOtherTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDef(taskDef);

            //in the same time for other thread:
            CompletableFuture.supplyAsync(() -> emulateParallelExecution(otherTaskEntity)).join();
        });

        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verify(mockedTask, Mockito.never()).onFailure(any(FailedExecutionContext.class));
        verifyLocalTaskIsFinished(taskEntity);
        verifyTaskIsCanceled(otherTaskEntity);
    }

    @Test
    void shouldNotCancelAllTaskByTaskDefWhenException() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelAllTaskByTaskDef(taskDef);
            throw new RuntimeException();
        });

        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyFirstAttemptTaskOnFailure(mockedTask, taskEntity);
        assertThat(taskRepository.find(otherTaskEntity.getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 1, "opt locking")
                .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "next retry time")
                .matches(te -> te.getAssignedWorker() == null, "empty assigned worker")
        ;
    }

    @Test
    void shouldApplyCommandsInNatureOrderForOtherTasksWhenFromContext() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId taskId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.reschedule(taskId, Duration.ofMinutes(1));
            distributedTaskService.cancelTaskExecution(taskId);
        });

        mockedTask = Mockito.spy(mockedTask);
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        assertThat(taskRepository.find(taskId.getId()))
                .isPresent()
                .get()
                .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled")
                .matches(task -> task.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 60L, "rescheduled")
        ;
    }
}
