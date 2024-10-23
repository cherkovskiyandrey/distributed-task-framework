package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.task.TaskGenerator;
import com.distributed_task_framework.task.TestTaskModel;
import com.distributed_task_framework.task.TestTaskModelSpec;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@Disabled
@FieldDefaults(level = AccessLevel.PROTECTED)
public abstract class BaseLocalWorkerIntegrationTest extends BaseSpringIntegrationTest {
    protected static final long CRON_NEXT_CALL_SEC = 50L;
    protected static final long CRON_NEXT_RETRY_AFTER_FAIL_SEC = 10L;

    //turn off taskRegistry
    @MockBean
    TaskRegistryService taskRegistryService;
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    TaskRepository taskRepository;
    @Autowired
    TaskMapper taskMapper;
    @Autowired
    TaskSerializer taskSerializer;
    @Autowired
    DistributedTaskService distributedTaskService;
    @Captor
    ArgumentCaptor<FailedExecutionContext<?>> failedArgumentCaptor;

    protected abstract TaskWorker getTaskWorker();

    protected record ActionBuilder<T>(
        TaskGenerator.Consumer<ExecutionContext<T>> action,
        TaskGenerator.Function<FailedExecutionContext<T>, Boolean> failureAction
    ) {
    }

    protected enum ActionMode {
        NORMAL,
        FAILED
    }

    protected <T> ActionBuilder<T> buildAction(TaskGenerator.Consumer<ExecutionContext<T>> action, ActionMode actionMode) {
        return switch (actionMode) {
            case NORMAL -> new ActionBuilder<>(action, m -> false);
            case FAILED -> new ActionBuilder<>(
                m -> {
                    throw new RuntimeException();
                },
                m -> {
                    try {
                        action.accept(m);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                });
        };
    }

    protected <T> TestTaskModel<T> buildActionAndGenerateTask(TaskGenerator.Consumer<ExecutionContext<T>> action,
                                                              Class<T> inputType,
                                                              ActionMode actionMode) {
        var actionBuilder = buildAction(action, actionMode);
        return extendedTaskGenerator.generate(TestTaskModelSpec.builder(inputType)
            .withSaveInstance()
            .action(actionBuilder.action())
            .failureAction(actionBuilder.failureAction())
            .build()
        );
    }

    protected void verifyTaskIsCanceled(TaskEntity taskEntity) {
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent()
            .get()
            .matches(task -> Boolean.TRUE.equals(task.isCanceled()), "canceled");
    }

    protected void verifyLocalTaskIsFinished(TaskEntity taskEntity) {
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        verifyTaskIsFinished(taskId);
    }

    protected void verifyOnlyOneTask(TaskDef<String> taskDef) {
        assertThat(taskRepository.findByName(taskDef.getTaskName(), 10)).hasSize(1);
    }

    protected void verifyTaskIsFinished(TaskId taskId) {
        Optional<TaskEntity> taskEntityOpt = taskRepository.find(taskId.getId());
        taskEntityOpt.ifPresent(entity -> assertThat(entity)
            .matches(
                taskEntity -> VirtualQueue.DELETED.equals(taskEntity.getVirtualQueue()),
                "has to be deleted"
            )
        );
    }

    protected void verifyTaskInNextAttempt(TaskId taskId, TaskSettings taskSettings) {
        long nextAttempt = taskSettings.getRetry().getFixed().getDelay().toSeconds();
        assertThat(taskRepository.find(taskId.getId())).isPresent()
            .get()
            .matches(te -> te.getFailures() == 1, "failures")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == nextAttempt, "reschedule time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    protected <T> void verifyIsEmptyByTaskDef(TaskDef<T> taskDef) {
        assertThat(taskRepository.findByName(taskDef.getTaskName(), 1)).isEmpty();
    }

    @SneakyThrows
    protected void verifyFirstAttemptTaskOnFailure(Task<String> mockedTask, TaskEntity taskEntity) {
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        //noinspection unchecked
        verify(mockedTask).onFailureWithResult((FailedExecutionContext<String>) failedArgumentCaptor.capture());

        assertThat(failedArgumentCaptor.getValue())
            .matches(fCtx -> fCtx.getFailures() == 1, "failures")
            .matches(fCtx -> !fCtx.isLastAttempt(), "last attempt")
            .matches(fCtx -> fCtx.getCurrentTaskId().equals(taskId), "taskId")
            .matches(fCtx -> fCtx.getWorkflowId().equals(taskEntity.getWorkflowId()), "workflowId")
            .matches(fCtx -> fCtx.getError() != null, "error != null")
            .matches(fCtx -> fCtx.getError().getClass().equals(RuntimeException.class), "error class")
            .matches(fCtx -> "hello message".equals(fCtx.getInputMessageOrThrow()), "message")
        ;
    }

    protected UUID emulateParallelExecution(TaskEntity taskEntity) {
        UUID foreignWorkerId = UUID.randomUUID();
        TaskEntity modifiedTaskEntity = taskEntity.toBuilder()
            .assignedWorker(foreignWorkerId)
            .executionDateUtc(LocalDateTime.now(clock))
            .build();
        taskRepository.saveOrUpdate(modifiedTaskEntity);
        return foreignWorkerId;
    }

    @SneakyThrows
    protected void verifyLastAttemptTaskOnFailure(Task<String> mockedTask, TaskEntity taskEntity) {
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());

        //noinspection unchecked
        verify(mockedTask).onFailureWithResult((FailedExecutionContext<String>) failedArgumentCaptor.capture());

        assertThat(failedArgumentCaptor.getValue())
            .matches(fCtx -> fCtx.getFailures() == 6, "failures")
            .matches(FailedExecutionContext::isLastAttempt, "last attempt")
            .matches(fCtx -> fCtx.getCurrentTaskId().equals(taskId), "taskId")
            .matches(fCtx -> fCtx.getWorkflowId().equals(taskEntity.getWorkflowId()), "workflowId")
            .matches(fCtx -> fCtx.getError() != null, "error != null")
            .matches(fCtx -> fCtx.getError().getClass().equals(RuntimeException.class), "error class")
            .matches(fCtx -> fCtx.getExecutionAttempt() == 6, "executionAttempt")
            .matches(fCtx -> "hello message".equals(fCtx.getInputMessageOrThrow()), "message")
        ;
    }

    protected void verifyLastAttemptCronTaskOnFailure(Task<Void> mockedTask, TaskEntity taskEntity) {
        verifyLastAttemptCronTaskOnFailure(mockedTask, taskEntity, 6);
    }

    @SneakyThrows
    protected void verifyLastAttemptCronTaskOnFailure(Task<Void> mockedTask, TaskEntity taskEntity, int failNumber) {
        //noinspection unchecked
        verify(mockedTask).onFailureWithResult((FailedExecutionContext<Void>) failedArgumentCaptor.capture());

        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        assertThat(failedArgumentCaptor.getValue())
            .matches(fCtx -> fCtx.getFailures() == failNumber, "failures")
            .matches(FailedExecutionContext::isLastAttempt, "last attempt")
            .matches(fCtx -> fCtx.getCurrentTaskId().equals(taskId), "taskId")
            .matches(fCtx -> fCtx.getWorkflowId().equals(taskEntity.getWorkflowId()), "workflowId")
            .matches(fCtx -> fCtx.getError() != null, "error != null")
            .matches(fCtx -> fCtx.getError().getClass().equals(RuntimeException.class), "error class")
        ;
    }

    @SneakyThrows
    protected void verifyFirstAttemptCronTaskOnFailure(Task<Void> mockedTask, TaskEntity taskEntity) {
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());

        //noinspection unchecked
        verify(mockedTask).onFailureWithResult((FailedExecutionContext<Void>) failedArgumentCaptor.capture());

        assertThat(failedArgumentCaptor.getValue())
            .matches(fCtx -> fCtx.getFailures() == 1, "failures")
            .matches(fCtx -> !fCtx.isLastAttempt(), "last attempt")
            .matches(fCtx -> fCtx.getCurrentTaskId().equals(taskId), "taskId")
            .matches(fCtx -> fCtx.getWorkflowId().equals(taskEntity.getWorkflowId()), "workflowId")
            .matches(fCtx -> fCtx.getError() != null, "error != null")
            .matches(fCtx -> fCtx.getError().getClass().equals(RuntimeException.class), "error class")
        ;
    }

    protected void verifyCronTaskInRepositoryToNextCall(TaskEntity taskEntity) {
        verifyCronTaskInRepository(taskEntity, CRON_NEXT_CALL_SEC);
    }

    protected void verifyCronTaskInRepositoryToNextRetry(TaskEntity taskEntity) {
        verifyCronTaskInRepository(taskEntity, CRON_NEXT_RETRY_AFTER_FAIL_SEC);
    }

    private void verifyCronTaskInRepository(TaskEntity taskEntity, long nextRetry) {
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == nextRetry, "next execution time")
            .matches(te -> te.getAssignedWorker() == null, "free assigned worker")
        ;
    }

    //use extendedTaskGenerator instead
    @Deprecated
    protected TaskEntity saveNewTaskEntity() {
        return saveBaseNewTaskEntity(0, UUID.randomUUID(), true);
    }

    //use extendedTaskGenerator instead
    @Deprecated
    protected TaskEntity saveNewTaskEntity(int failures) {
        return saveBaseNewTaskEntity(failures, UUID.randomUUID(), false);
    }

    //use extendedTaskGenerator instead
    @Deprecated
    protected TaskEntity saveNewChildInSameWorkflow(TaskEntity parentTaskEntity) {
        return saveBaseNewTaskEntity(0, parentTaskEntity.getWorkflowId(), false);
    }

    @SneakyThrows
    private TaskEntity saveBaseNewTaskEntity(int failures, UUID workflowId, boolean isJoin) {
        TaskEntity taskEntity = TaskEntity.builder()
            .taskName("test")
            .id(UUID.randomUUID())
            .workflowId(workflowId)
            .virtualQueue(VirtualQueue.NEW)
            .workflowCreatedDateUtc(LocalDateTime.now(clock))
            .createdDateUtc(LocalDateTime.now(clock))
            .executionDateUtc(LocalDateTime.now(clock))
            .messageBytes(taskSerializer.writeValue("hello message"))
            .failures(failures)
            .notToPlan(isJoin)
            .build();
        return taskRepository.saveOrUpdate(taskEntity);
    }

    protected void emulateParallelExecutionCronTask(TaskEntity taskEntity) {
        TaskEntity modifiedTaskEntity = taskEntity.toBuilder()
            .assignedWorker(null)
            .executionDateUtc(LocalDateTime.now(clock).plusSeconds(CRON_NEXT_CALL_SEC))
            .build();
        taskRepository.saveOrUpdate(modifiedTaskEntity);
    }

    protected void verifyParallelExecution(TaskEntity taskEntity, UUID foreignWorkerId) {
        Assertions.assertThat(taskRepository.find(taskEntity.getId())).isPresent()
            .get()
            .matches(te -> te.getVersion() == 2, "opt locking")
            .matches(te -> te.getExecutionDateUtc().toEpochSecond(ZoneOffset.UTC) == 0L, "execution time")
            .matches(te -> foreignWorkerId.equals(te.getAssignedWorker()), "foreign assigned worker")
        ;
    }

    protected List<TestTaskModel<String>> generateIndependentTasksInTheSameWorkflow(int number) {
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
}
