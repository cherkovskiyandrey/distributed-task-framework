package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.task.TestTaskModel;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;


//todo: adapt!!!
@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractCancelWorkflowByTaskId extends BaseLocalWorkerIntegrationTest {

    //todo: think about moving to BaseLocalWorkerIntegrationTest or other places
    private List<TestTaskModel<String>> generateIndependentTasksInTheSameWorkflow(int number) {
        var firstTaskModel = extendedTaskGenerator.generateDefaultAndSave(String.class);
        if (number == 1) {
            return List.of(firstTaskModel);
        }
        var others = IntStream.range(0, number - 1)
            .mapToObj(i -> extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
                .withSaveInstance()
                .withSameWorkflowAs(firstTaskModel.getTaskId())
                .build()
            ))
            .toList();

        return ImmutableList.<TestTaskModel<String>>builder()
            .add(firstTaskModel)
            .addAll(others)
            .build();
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    @Test
    void shouldCancelWorkflowByTaskId() {
        //when
        var taskModels = generateIndependentTasksInTheSameWorkflow(10);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> distributedTaskService.cancelWorkflowByTaskId(taskModels.get(0).getTaskId()))
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verify(rootTaskModel.getMockedTask(), Mockito.never()).onFailure(any(FailedExecutionContext.class));
        verifyTaskIsFinished(rootTaskModel.getTaskId());
        taskModels.forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
    }

    @Test
    void shouldNotCancelAllTaskByTaskDefWhenParallelExecution() {
        //when
        setFixedTime();

        var taskModels = generateIndependentTasksInTheSameWorkflow(10);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> distributedTaskService.cancelWorkflowByTaskId(taskModels.get(0).getTaskId()))
            .build()
        );
        UUID foreignWorkerId = emulateParallelExecution(rootTaskModel.getTaskEntity());

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyParallelExecution(rootTaskModel.getTaskEntity(), foreignWorkerId);
        taskModels.forEach(taskModel ->
            assertThat(taskRepository.find(taskModel.getTaskId().getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 1, "version")
                .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "execution time")
                .matches(te -> !foreignWorkerId.equals(te.getAssignedWorker()), "foreign assigned worker")
        );
    }

    //todo
    @Test
    void shouldCancelAllTaskByTaskDefOtherTaskAndNotCurrentWhenParallelExecutionAndImmediately() {
//        //when
//        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
//        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
//
//        setFixedTime();
//        TaskEntity taskEntity = saveNewTaskEntity();
//        TaskEntity otherTaskEntity = saveNewTaskEntity();
//        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
//            distributedTaskService.cancelAllTaskByTaskDefImmediately(taskDef);
//            verifyTaskIsCanceled(otherTaskEntity);
//        });
//
//        mockedTask = Mockito.spy(mockedTask);
//        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
//
//        UUID foreignWorkerId = emulateParallelExecution(taskEntity);
//
//        //do
//        getTaskWorker().execute(taskEntity, registeredTask);
//
//        //verify
//        verifyParallelExecution(taskEntity, foreignWorkerId);
//        verifyTaskIsCanceled(otherTaskEntity);
    }

    //todo
    @SuppressWarnings("unchecked")
    @Test
    void shouldCancelAllTaskByTaskDefWhenParallelExecutionOfOtherTask() {
//        //when
//        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
//        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
//
//        setFixedTime();
//        TaskEntity otherTaskEntity = saveNewTaskEntity();
//        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
//            distributedTaskService.cancelAllTaskByTaskDef(taskDef);
//
//            //in the same time for other thread:
//            CompletableFuture.supplyAsync(() -> emulateParallelExecution(otherTaskEntity)).join();
//        });
//
//        mockedTask = Mockito.spy(mockedTask);
//        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
//        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
//
//        TaskEntity taskEntity = saveNewTaskEntity();
//
//        //do
//        getTaskWorker().execute(taskEntity, registeredTask);
//
//        //verify
//        verify(mockedTask, Mockito.never()).onFailure(any(FailedExecutionContext.class));
//        verifyLocalTaskIsFinished(taskEntity);
//        verifyTaskIsCanceled(otherTaskEntity);
    }

    //todo
    @Test
    void shouldNotCancelAllTaskByTaskDefWhenException() {
//        //when
//        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
//        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
//
//        setFixedTime();
//        TaskEntity otherTaskEntity = saveNewTaskEntity();
//        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
//            distributedTaskService.cancelAllTaskByTaskDef(taskDef);
//            throw new RuntimeException();
//        });
//
//        mockedTask = Mockito.spy(mockedTask);
//        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
//        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
//
//        TaskEntity taskEntity = saveNewTaskEntity();
//
//        //do
//        getTaskWorker().execute(taskEntity, registeredTask);
//
//        //verify
//        verifyFirstAttemptTaskOnFailure(mockedTask, taskEntity);
//        assertThat(taskRepository.find(otherTaskEntity.getId())).isPresent()
//            .get()
//            .matches(te -> te.getVersion() == 1, "opt locking")
//            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "next retry time")
//            .matches(te -> te.getAssignedWorker() == null, "empty assigned worker")
//        ;
    }

    //todo
    @Test
    void shouldApplyCommandsInNatureOrderForOtherTasksWhenFromContext() {
//        //when
//        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
//        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
//
//        setFixedTime();
//        TaskEntity otherTaskEntity = saveNewTaskEntity();
//        TaskId taskId = taskMapper.map(otherTaskEntity, commonSettings.getAppName());
//        Task<String> mockedTask = TaskGenerator.defineTask(taskDef, m -> {
//            distributedTaskService.reschedule(taskId, Duration.ofMinutes(1));
//            distributedTaskService.cancelTaskExecution(taskId);
//        });
//
//        mockedTask = Mockito.spy(mockedTask);
//        RegisteredTask<String> registeredTask = RegisteredTask.of(mockedTask, taskSettings);
//        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));
//
//        TaskEntity taskEntity = saveNewTaskEntity();
//
//        //do
//        getTaskWorker().execute(taskEntity, registeredTask);
//
//        //verify
//        verifyLocalTaskIsFinished(taskEntity);
//        assertThat(taskRepository.find(taskId.getId()))
//            .isPresent()
//            .get()
//            .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled")
//            .matches(task -> task.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 60L, "rescheduled")
//        ;
    }
}
