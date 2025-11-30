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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractCancelAllWorkflowByTaskIdTest extends BaseLocalWorkerIntegrationTest {

    @SuppressWarnings("DataFlowIssue")
    @SneakyThrows
    @Test
    void shouldCancelAllWorkflowByTaskIdIncludedCurrentOne() {
        //when
        var cancelingTaskModelGroupOne = generateIndependentTasksInTheSameWorkflow(10);
        var cancelingTaskModelGroupTwo = generateIndependentTasksInTheSameWorkflow(10);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .withSameWorkflowAs(cancelingTaskModelGroupOne.get(0).getTaskId())
            .action(ctx -> {
                    distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofHours(1));
                    distributedTaskService.cancelAllWorkflowsByTaskId(
                        List.of(
                            cancelingTaskModelGroupOne.get(0).getTaskId(),
                            cancelingTaskModelGroupTwo.get(0).getTaskId()
                        )
                    );
                }
            )
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(rootTaskModel.getTaskId());
        cancelingTaskModelGroupOne.forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
        cancelingTaskModelGroupTwo.forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
    }

    @Test
    void shouldNotCancelAllWorkflowByTaskIdWhenParallelExecution() {
        //when
        setFixedTime();

        var taskModelsGroupOne = generateIndependentTasksInTheSameWorkflow(10);
        var taskModelsGroupTwo = generateIndependentTasksInTheSameWorkflow(10);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> distributedTaskService.cancelAllWorkflowsByTaskId(
                List.of(
                    taskModelsGroupOne.get(0).getTaskId(),
                    taskModelsGroupTwo.get(0).getTaskId()
                )
            ))
            .build()
        );
        UUID foreignWorkerId = emulateParallelExecution(rootTaskModel.getTaskEntity());

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyParallelExecution(rootTaskModel.getTaskEntity(), foreignWorkerId);
        Stream.concat(taskModelsGroupOne.stream(), taskModelsGroupTwo.stream()).forEach(taskModel ->
            assertThat(taskRepository.find(taskModel.getTaskId().getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 1, "version")
                .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "execution time")
                .matches(te -> !foreignWorkerId.equals(te.getAssignedWorker()), "foreign assigned worker")
        );
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldCancelAllWorkflowByTaskIdWhenParallelExecutionAndImmediately() {
        //when
        setFixedTime();

        var taskModelsGroupOne = generateIndependentTasksInTheSameWorkflow(10);
        var taskModelsGroupTwo = generateIndependentTasksInTheSameWorkflow(10);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.cancelAllWorkflowsByTaskIdImmediately(
                    List.of(
                        taskModelsGroupOne.get(0).getTaskId(),
                        taskModelsGroupTwo.get(0).getTaskId()
                    )
                );
                Stream.concat(taskModelsGroupOne.stream(), taskModelsGroupTwo.stream())
                    .forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
            })
            .build()
        );
        UUID foreignWorkerId = emulateParallelExecution(rootTaskModel.getTaskEntity());

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyParallelExecution(rootTaskModel.getTaskEntity(), foreignWorkerId);
        Stream.concat(taskModelsGroupOne.stream(), taskModelsGroupTwo.stream())
            .forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    @Test
    void shouldCancelAllWorkflowByTaskIdWhenParallelExecutionOfOtherTask() {
        //when
        setFixedTime();

        var taskModelOne = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var taskModelTwo = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.cancelAllWorkflowsByTaskId(List.of(taskModelOne.getTaskId(), taskModelTwo.getTaskId()));
                CompletableFuture.supplyAsync(() -> emulateParallelExecution(taskModelOne.getTaskEntity())).join();
                CompletableFuture.supplyAsync(() -> emulateParallelExecution(taskModelTwo.getTaskEntity())).join();
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verify(rootTaskModel.getMockedTask(), Mockito.never()).onFailure(any(FailedExecutionContext.class));
        verifyLocalTaskIsFinished(rootTaskModel.getTaskEntity());
        verifyTaskIsCanceled(taskModelOne.getTaskEntity());
        verifyTaskIsCanceled(taskModelTwo.getTaskEntity());
    }

    @Test
    void shouldNotCancelAllWorkflowByTaskIdWhenException() {
        //when
        setFixedTime();

        var taskModelOne = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var taskModelTwo = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var taskModels = List.of(taskModelOne.getTaskId(), taskModelTwo.getTaskId());
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.cancelAllWorkflowsByTaskId(taskModels);
                throw new RuntimeException();
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyFirstAttemptTaskOnFailure(rootTaskModel.getMockedTask(), rootTaskModel.getTaskEntity());
        taskModels.forEach(taskModel ->
            assertThat(taskRepository.find(taskModel.getId())).isPresent()
                .get()
                .matches(te -> te.getVersion() == 1, "version")
                .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "execution time")
        );
    }

    @Test
    void shouldApplyCommandsInNatureOrderForOtherTasksWhenFromContext() {
        //when
        setFixedTime();

        var taskModelOne = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var taskModelTwo = generateIndependentTasksInTheSameWorkflow(1).get(0);
        var taskModels = List.of(taskModelOne.getTaskId(), taskModelTwo.getTaskId());
        var rootTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .action(ctx -> {
                distributedTaskService.reschedule(taskModelOne.getTaskId(), Duration.ofMinutes(1));
                distributedTaskService.reschedule(taskModelTwo.getTaskId(), Duration.ofMinutes(1));
                distributedTaskService.cancelAllWorkflowsByTaskId(taskModels);
            })
            .build()
        );

        //do
        getTaskWorker().execute(rootTaskModel.getTaskEntity(), rootTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(rootTaskModel.getTaskId());
        taskModels.forEach(taskModel ->
            assertThat(taskRepository.find(taskModel.getId())).isPresent()
                .get()
                .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled")
                .matches(task -> task.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "rescheduled")
        );
    }
}
