package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;


@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class AbstractCancelWorkflowByTaskIdTest extends BaseLocalWorkerIntegrationTest {

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldCancelWorkflowByTaskIdIncludeCurrentOne() {
        //when
        var cancelingTaskModels = generateIndependentTasksInTheSameWorkflow(10);
        var executionTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .withSameWorkflowAs(cancelingTaskModels.get(0).getTaskId())
            .action(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofHours(1));
                distributedTaskService.cancelWorkflowByTaskId(cancelingTaskModels.get(0).getTaskId());
            })
            .build()
        );

        //do
        getTaskWorker().execute(executionTaskModel.getTaskEntity(), executionTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(executionTaskModel.getTaskId());
        verifyTaskIsCanceled(cancelingTaskModels.get(0).getTaskEntity());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldCancelWorkflowByTaskIdIncludeCurrentOneWhenCronTask() {
        //when
        var cancelingTaskModels = generateIndependentTasksInTheSameWorkflow(10);
        var executionTaskModel = extendedTaskGenerator.generate(TestTaskModelSpec.builder(String.class)
            .withSaveInstance()
            .recurrent()
            .withSameWorkflowAs(cancelingTaskModels.get(0).getTaskId())
            .action(ctx -> {
                distributedTaskService.reschedule(ctx.getCurrentTaskId(), Duration.ofHours(1));
                distributedTaskService.cancelWorkflowByTaskId(cancelingTaskModels.get(0).getTaskId());
            })
            .build()
        );

        //do
        getTaskWorker().execute(executionTaskModel.getTaskEntity(), executionTaskModel.getRegisteredTask());

        //verify
        verifyTaskIsFinished(executionTaskModel.getTaskId());
        cancelingTaskModels.forEach(taskModel -> verifyTaskIsCanceled(taskModel.getTaskEntity()));
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

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldChangeExecutionDateToNowWhenCancelWorkflow() {
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
            .action(ctx -> distributedTaskService.cancelWorkflowByTaskId(cancelingTaskModel.getTaskId()))
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
