package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractNestedLocalCancelTest extends BaseLocalWorkerIntegrationTest {

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void shouldNotModifyOtherTaskWhenCurrentIsCanceledParallel() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecution(otherTakId);
            CompletableFuture.supplyAsync(() -> {
                try {
                    return distributedTaskService.cancelTaskExecution(m.getCurrentTaskId());
                } catch (Exception exception) {
                    throw new RuntimeException(exception);
                }
            }).get();
            assertThat(taskRepository.find(otherTakId.getId())).isPresent();
        });
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        assertThat(taskRepository.find(taskId.getId())).isPresent()
                .get()
                .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled")
                .matches(task -> task.getAssignedWorker() == null, "worker cleaned")
        ;
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
                .get()
                .matches(task -> !Boolean.TRUE.equals(task.isCanceled()), "not canceled")
        ;

        //do
        taskEntity = taskRepository.find(taskId.getId()).get();
        getTaskWorker().execute(taskEntity, registeredTask); //second time execution of canceled task

        //verify
        verifyLocalTaskIsFinished(taskEntity);
    }

    @Test
    void shouldAllowToCancelOtherTaskWhenFromTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecution(otherTakId);
            assertThat(taskRepository.find(otherTakId.getId())).isPresent();
        });
        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
        verifyTaskIsCanceled(otherTaskEntity);
    }

    @Test
    void shouldNotModifyTaskWhenParallelExecutionAndCancel() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecution(m.getCurrentTaskId());
            distributedTaskService.cancelTaskExecution(otherTakId);
            assertThat(taskRepository.find(otherTakId.getId())).isPresent();
        });
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<Void>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();
        emulateParallelExecutionCronTask(taskEntity);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyCronTaskInRepositoryToNextCall(taskEntity);
        assertThat(taskRepository.find(otherTakId.getId())).isPresent()
                .get()
                .matches(task -> !Boolean.TRUE.equals(task.isCanceled()), "not canceled");
    }

    @Test
    void shouldModifyOtherTaskAndNotCurrentOneWhenParallelExecutionAndCancel() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();

        setFixedTime();
        TaskEntity otherTaskEntity = saveNewTaskEntity();
        TaskId otherTakId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecutionImmediately(m.getCurrentTaskId());
            distributedTaskService.cancelTaskExecutionImmediately(otherTakId);
            verifyTaskIsCanceled(otherTaskEntity);
        });
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
        when(taskRegistryService.<Void>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();
        emulateParallelExecutionCronTask(taskEntity);

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyCronTaskInRepositoryToNextCall(taskEntity);
        verifyTaskIsCanceled(otherTaskEntity);
    }

    @Test
    void shouldNotModifyTaskWhenCancelAndException() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecution(m.getCurrentTaskId());
            throw new RuntimeException();
        });
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyCronTaskInRepositoryToNextRetry(taskEntity);
    }

    @Test
    void shouldAllowToCancelCronOwnTaskWhenFromTask() {
        //when
        TaskDef<Void> taskDef = TaskDef.privateTaskDef("test-cron", Void.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();
        Task<Void> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
            distributedTaskService.cancelTaskExecution(m.getCurrentTaskId());
        });
        RegisteredTask<Void> registeredTask = RegisteredTask.of(mockedTask, taskSettings);

        setFixedTime();
        TaskEntity taskEntity = saveNewTaskEntity();

        //do
        getTaskWorker().execute(taskEntity, registeredTask);

        //verify
        verifyLocalTaskIsFinished(taskEntity);
    }
}
