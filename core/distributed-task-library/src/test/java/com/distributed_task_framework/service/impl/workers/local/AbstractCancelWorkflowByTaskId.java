package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;


@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractCancelWorkflowByTaskId extends BaseLocalWorkerIntegrationTest {

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
    void shouldNotCancelWorkflowByTaskIdWhenParallelExecution() {
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

    @Test
    void shouldCancelWorkflowByTaskIdWhenParallelExecutionAndImmediately() {
        //when
        setFixedTime();

        var taskModels = generateIndependentTasksInTheSameWorkflow(10);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.cancelWorkflowByTaskIdImmediately(taskModels.get(0).getTaskId());
                taskModels.forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
            })
            .build()
        );
        UUID foreignWorkerId = emulateParallelExecution(rootTaskModel.getTaskEntity());

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyParallelExecution(rootTaskModel.getTaskEntity(), foreignWorkerId);
        taskModels.forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldCancelWorkflowByTaskIdWhenParallelExecutionOfOtherTask() {
        //when
        setFixedTime();

        var taskModel = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.cancelWorkflowByTaskId(taskModel.getTaskId());
                CompletableFuture.supplyAsync(() -> emulateParallelExecution(taskModel.getTaskEntity())).join();
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verify(rootTaskModel.getMockedTask(), Mockito.never()).onFailure(any(FailedExecutionContext.class));
        verifyLocalTaskIsFinished(rootTaskModel.getTaskEntity());
        verifyTaskIsCanceled(taskModel.getTaskEntity());
    }

    @Test
    void shouldNotCancelWorkflowByTaskIdWhenException() {
        //when
        setFixedTime();

        var taskModel = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.cancelWorkflowByTaskId(taskModel.getTaskId());
                throw new RuntimeException();
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyFirstAttemptTaskOnFailure(rootTaskModel.getMockedTask(), rootTaskModel.getTaskEntity());
        assertThat(taskRepository.find(taskModel.getTaskId().getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 1, "version")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "execution time");
    }

    @Test
    void shouldApplyCommandsInNatureOrderForOtherTasksWhenFromContext() {
        //when
        setFixedTime();

        var taskModel = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.reschedule(taskModel.getTaskId(), Duration.ofMinutes(1));
                distributedTaskService.cancelWorkflowByTaskId(taskModel.getTaskId());
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(rootTaskModel.getTaskId());
        assertThat(taskRepository.find(taskModel.getTaskId().getId())).isPresent()
            .get()
            .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled")
            .matches(task -> task.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 60L, "rescheduled");
    }
}
