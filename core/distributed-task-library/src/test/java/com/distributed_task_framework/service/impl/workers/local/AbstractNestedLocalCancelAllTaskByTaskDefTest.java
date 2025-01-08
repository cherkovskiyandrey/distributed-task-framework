package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
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
import org.mockito.Mockito;

import java.time.Duration;
import java.time.LocalDateTime;
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

    @SuppressWarnings("DataFlowIssue")
    @SneakyThrows
    @Test
    void shouldCancelAllTaskByTaskDefIncludeCurrentOne() {
        //when
        var cancelingTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .build()
        );
        var executionTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(cancelingTaskModel.getTaskDef())
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofHours(1));
                distributedTaskService.cancelAllTaskByTaskDef(cancelingTaskModel.getTaskDef());
            })
            .build()
        );

        //do
        getTaskWorker().execute(executionTaskModel.getTaskEntity(), executionTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(executionTaskModel.getTaskId());
        verifyTaskIsCanceled(cancelingTaskModel.getTaskEntity());
    }

    @SuppressWarnings("DataFlowIssue")
    @SneakyThrows
    @Test
    void shouldCancelAllTaskByTaskDefAndNextInvocationWhenCronTask() {
        //when
        var cancelingTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .build()
        );
        var executionTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(cancelingTaskModel.getTaskDef())
            .withSaveInstance()
            .recurrent()
            .action(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofHours(1));
                distributedTaskService.cancelAllTaskByTaskDef(cancelingTaskModel.getTaskDef());
            })
            .build()
        );

        //do
        getTaskWorker().execute(executionTaskModel.getTaskEntity(), executionTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(executionTaskModel.getTaskId());
        verifyTaskIsCanceled(cancelingTaskModel.getTaskEntity());
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

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldChangeExecutionDateToNowWhenCancelAllTaskByTaskDef() {
        //when
        setFixedTime();
        var cancelingTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .taskEntityCustomizer(taskEntity -> taskEntity.toBuilder()
                .executionDateUtc(LocalDateTime.now(clock).plusHours(1))
                .build()
            )
            .build()
        );
        var executionTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> distributedTaskService.cancelAllTaskByTaskDef(cancelingTaskModel.getTaskDef()))
            .build()
        );

        //do
        getTaskWorker().execute(executionTaskModel.getTaskEntity(), executionTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(executionTaskModel.getTaskId());
        assertThat(taskRepository.find(cancelingTaskModel.getTaskId().getId()))
            .isPresent()
            .get()
            .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled")
            .matches(task -> task.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "execution now")
        ;
    }
}
