package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.TaskGenerator;
import com.distributed_task_framework.task.TestTaskModel;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class LocalTaskCommandServiceImplIntegrationTest extends BaseSpringIntegrationTest {
    //turn off taskRegistry
    @MockBean
    TaskRegistryService taskRegistryService;
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    DistributedTaskService distributedTaskService;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    TaskSerializer taskSerializer;

    @SneakyThrows
    @Test
    void shouldHandleParameterizedTypeTreadSafely() {
        //when
        TaskDef<List<String>> taskDef = TaskDef.privateTaskDef("test", new TypeReference<>() {
        });
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        RegisteredTask<List<String>> registeredTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(taskDef), taskSettings);
        when(taskRegistryService.<List<String>>getRegisteredLocalTask(eq("test")))
            .thenReturn(Optional.of(registeredTask));

        //do
        TaskId taskId = distributedTaskService.schedule(taskDef, ExecutionContext.simple(List.of("hello", "world")));

        //verify
        //noinspection unchecked
        Assertions.assertThat(taskRepository.find(taskId.getId()))
            .isPresent()
            .get()
            .satisfies(taskEntity -> assertThat((List<String>) taskSerializer.readValue(
                        taskEntity.getMessageBytes(),
                        taskDef.getInputMessageType()
                    )
                ).containsExactlyInAnyOrder("hello", "world")
            );
    }

    @SneakyThrows
    @Test
    void shouldCreateJoinTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        RegisteredTask<String> registeredTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(taskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredCronTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredCronTask));

        //do
        Pair<TaskId, TaskId> taskIds = new TransactionTemplate(transactionManager).execute(transactionStatus -> {
            try {
                TaskId taskId = distributedTaskService.schedule(taskDef, ExecutionContext.simple("general"));
                TaskId joinTaskId = distributedTaskService.scheduleJoin(joinTaskDef, ExecutionContext.simple("join"), List.of(taskId));

                return Pair.of(taskId, joinTaskId);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });

        //verify
        assertThat(taskIds).isNotNull();
        TaskId taskId = taskIds.getLeft();
        TaskId joinTaskId = taskIds.getRight();

        Assertions.assertThat(taskRepository.find(taskId.getId())).isPresent()
            .get()
            .matches(task -> !Boolean.TRUE.equals(task.isNotToPlan()), "allowed to plan");
        Assertions.assertThat(taskRepository.find(joinTaskId.getId())).isPresent()
            .get()
            .matches(task -> Boolean.TRUE.equals(task.isNotToPlan()), "not allowed to plan");
        Assertions.assertThat(taskLinkRepository.findAllByJoinTaskIdIn(List.of(joinTaskId.getId())))
            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
            .containsExactlyInAnyOrder(
                toJoinTaskLink(joinTaskId, taskId)
            );
    }

    @SneakyThrows
    @Test
    void shouldNotCreateJoinTaskWhenNotInTransaction() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();

        RegisteredTask<String> registeredTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(taskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredCronTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredCronTask));

        //do & verify
        TaskId taskId = distributedTaskService.schedule(taskDef, ExecutionContext.simple("general"));
        assertThatThrownBy(() -> distributedTaskService.scheduleJoin(joinTaskDef, ExecutionContext.simple("join"), List.of(taskId)))
            .isInstanceOf(IllegalStateException.class);

    }

    @SneakyThrows
    @Test
    void shouldNotScheduleCronJoinTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskSettings taskSettings = defaultTaskSettings.toBuilder().build();
        TaskSettings joinTaskSettings = newRecurrentTaskSettings();

        RegisteredTask<String> registeredTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(taskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredCronTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), joinTaskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredCronTask));

        //do & verify
        new TransactionTemplate(transactionManager).executeWithoutResult(transactionStatus -> {
            final TaskId taskId;
            try {
                taskId = distributedTaskService.schedule(taskDef, ExecutionContext.simple("general"));
            } catch (Exception exception) {
                throw new RuntimeException();
            }
            assertThatThrownBy(() -> distributedTaskService.scheduleJoin(joinTaskDef, ExecutionContext.simple("join"), List.of(taskId)))
                .isInstanceOf(TaskConfigurationException.class);
        });
    }

    @SneakyThrows
    @Test
    void shouldNotScheduleJoinTaskWhenJoinToCronTask() {
        //when
        TaskDef<String> taskDef = TaskDef.privateTaskDef("test", String.class);
        TaskDef<String> joinTaskDef = TaskDef.privateTaskDef("join-task", String.class);
        TaskSettings taskSettings = newRecurrentTaskSettings();
        TaskSettings joinTaskSettings = defaultTaskSettings.toBuilder().build();

        RegisteredTask<String> registeredTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(taskDef), taskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("test"))).thenReturn(Optional.of(registeredTask));

        RegisteredTask<String> registeredCronTask = RegisteredTask.of(TaskGenerator.emptyDefineTask(joinTaskDef), joinTaskSettings);
        when(taskRegistryService.<String>getRegisteredLocalTask(eq("join-task"))).thenReturn(Optional.of(registeredCronTask));

        //do & verify
        new TransactionTemplate(transactionManager).executeWithoutResult(transactionStatus -> {
            final TaskId taskId;
            try {
                taskId = distributedTaskService.schedule(taskDef, ExecutionContext.simple("general"));
            } catch (Exception exception) {
                throw new RuntimeException();
            }
            assertThatThrownBy(() -> distributedTaskService.scheduleJoin(joinTaskDef, ExecutionContext.simple("join"), List.of(taskId)))
                .isInstanceOf(TaskConfigurationException.class);
        });
    }

    @SneakyThrows
    @Test
    void shouldWaitCompletion() {
        //when
        var cyclicBarrier = new CyclicBarrier(2);
        var testTaskModel = extendedTaskGenerator.generateDefaultAndSave(String.class);
        CompletableFuture.runAsync(() -> assertThatNoException().isThrownBy(() -> {
                    cyclicBarrier.await();
                    TimeUnit.SECONDS.sleep(5);
                    internalTaskCommandService.finalize(testTaskModel.getTaskEntity());
                }
            )
        );

        //do
        cyclicBarrier.await();
        distributedTaskService.waitCompletion(testTaskModel.getTaskId());

        //verify
        verifyTaskIsFinished(Objects.requireNonNull(testTaskModel.getTaskId()));
    }

    @SneakyThrows
    @Test
    void shouldThrowExceptionWhenWaitCompletionWithTimeoutAndTimeoutExpired() {
        //when
        var testTaskModel = extendedTaskGenerator.generateDefaultAndSave(String.class);

        //do & verify
        assertThatThrownBy(() -> distributedTaskService.waitCompletion(testTaskModel.getTaskId(), Duration.ofSeconds(5)))
            .isInstanceOf(TimeoutException.class);
    }

    @SneakyThrows
    @Test
    void shouldWaitCompletionAllWorkflow() {
        //when
        var cyclicBarrier = new CyclicBarrier(2);
        var testTaskModels = generateIndependentTasksInTheSameWorkflow(5);
        CompletableFuture.runAsync(() -> assertThatNoException().isThrownBy(() -> {
                    cyclicBarrier.await();
                    for (var testTaskModel : testTaskModels) {
                        TimeUnit.SECONDS.sleep(1);
                        internalTaskCommandService.finalize(testTaskModel.getTaskEntity());
                    }
                }
            )
        );

        //do
        cyclicBarrier.await();
        distributedTaskService.waitCompletionAllWorkflow(testTaskModels.get(0).getTaskId());

        //verify
        testTaskModels.forEach(testTaskModel -> verifyTaskIsFinished(Objects.requireNonNull(testTaskModel.getTaskId())));
    }

    @SneakyThrows
    @Test
    void shouldThrowExceptionWhenWaitCompletionAllWorkflowWithTimeoutAndTimeoutExpired() {
        //when
        var testTaskModel = extendedTaskGenerator.generateDefaultAndSave(String.class);

        //do & verify
        assertThatThrownBy(() -> distributedTaskService.waitCompletionAllWorkflow(
                testTaskModel.getTaskId(),
                Duration.ofSeconds(3)
            )
        )
            .isInstanceOf(TimeoutException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @SneakyThrows
    @Test
    void shouldWaitCompletionAllWorkflows() {
        //when
        var cyclicBarrier = new CyclicBarrier(2);
        var testTaskModelsOne = generateIndependentTasksInTheSameWorkflow(5);
        var testTaskModelsTwo = generateIndependentTasksInTheSameWorkflow(5);
        CompletableFuture.runAsync(() -> assertThatNoException().isThrownBy(() -> {
                    cyclicBarrier.await();
                    for (var testTaskModel : ImmutableList.<TestTaskModel<String>>builder()
                        .addAll(testTaskModelsOne)
                        .addAll(testTaskModelsTwo)
                        .build()) {
                        TimeUnit.MILLISECONDS.sleep(500);
                        internalTaskCommandService.finalize(testTaskModel.getTaskEntity());
                    }
                }
            )
        );

        //do
        cyclicBarrier.await();
        distributedTaskService.waitCompletionAllWorkflows(List.of(
                testTaskModelsOne.get(0).getTaskId(),
                testTaskModelsTwo.get(0).getTaskId()
            )
        );

        //verify
        testTaskModelsOne.forEach(testTaskModel -> verifyTaskIsFinished(Objects.requireNonNull(testTaskModel.getTaskId())));
        testTaskModelsTwo.forEach(testTaskModel -> verifyTaskIsFinished(Objects.requireNonNull(testTaskModel.getTaskId())));
    }

    @SuppressWarnings("DataFlowIssue")
    @SneakyThrows
    @Test
    void shouldThrowExceptionWhenWaitCompletionAllWorkflowsWithTimeoutAndTimeoutExpired() {
        //when
        var testTaskModelOne = extendedTaskGenerator.generateDefaultAndSave(String.class);
        var testTaskModelTwo = extendedTaskGenerator.generateDefaultAndSave(String.class);

        //do & verify
        assertThatThrownBy(() -> distributedTaskService.waitCompletionAllWorkflows(
                List.of(testTaskModelOne.getTaskId(), testTaskModelTwo.getTaskId()),
                Duration.ofSeconds(3)
            )
        )
            .isInstanceOf(TimeoutException.class);
    }
}
