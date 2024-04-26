package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.model.WorkerContext;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.task.common.RemoteStubTask;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import com.distributed_task_framework.persistence.entity.RemoteCommandEntity;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.service.impl.remote_commands.CancelTaskByTaskDefCommand;
import com.distributed_task_framework.service.impl.remote_commands.CancelTaskCommand;
import com.distributed_task_framework.service.impl.remote_commands.RescheduleByTaskDefCommand;
import com.distributed_task_framework.service.impl.remote_commands.RescheduleCommand;
import com.distributed_task_framework.service.impl.remote_commands.ScheduleCommand;
import com.distributed_task_framework.service.internal.TaskCommandWithDetectorService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.WorkerContextManager;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RemoteTaskCommandServiceImpl extends AbstractTaskCommandWithDetectorService implements TaskCommandWithDetectorService {
    RemoteCommandRepository remoteCommandRepository;
    TaskSerializer taskSerializer;
    TaskRegistryService taskRegistryService;
    Clock clock;

    public RemoteTaskCommandServiceImpl(WorkerContextManager workerContextManager,
                                        PlatformTransactionManager transactionManager,
                                        RemoteCommandRepository remoteCommandRepository,
                                        TaskSerializer taskSerializer,
                                        TaskRegistryService taskRegistryService,
                                        Clock clock) {
        super(workerContextManager, transactionManager);
        this.remoteCommandRepository = remoteCommandRepository;
        this.taskSerializer = taskSerializer;
        this.taskRegistryService = taskRegistryService;
        this.clock = clock;
    }

    @Override
    public <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return executeTxAware(
                () -> schedule(taskDef, executionContext, Duration.ZERO, false),
                false
        );
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return executeTxAware(
                () -> schedule(taskDef, executionContext, Duration.ZERO, true),
                true
        );
    }

    @Override
    public <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return executeTxAware(
                () -> schedule(taskDef, executionContext, delay, false),
                false
        );
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        throw new UnsupportedOperationException("Isn't supported yet!");
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return executeTxAware(
                () -> schedule(taskDef, executionContext, delay, true),
                true
        );
    }

    private <T> TaskId schedule(TaskDef<T> taskDef,
                                ExecutionContext<T> executionContext,
                                Duration delay,
                                boolean isImmediately) throws Exception {
        TaskId taskId = new TaskId(
                taskDef.getAppName(),
                taskDef.getTaskName(),
                //we generate remote task id locally
                UUID.randomUUID(),
                executionContext.getWorkflowId()
        );
        executionContext = executionContext.toBuilder()
                .currentTaskId(taskId)
                .build();
        var command = ScheduleCommand.<T>builder()
                //todo: it is a bad idea, because in case when remote server don't know nothing about
                //JavaType inputMessageType in taskDef it will lead to error during deserialization
                .taskDef(taskDef)
                .executionContext(executionContext)
                .delay(delay)
                .build();
        byte[] bytes = taskSerializer.writeValue(command);
        var commandEntity = RemoteCommandEntity.builder()
                .appName(taskDef.getAppName())
                .taskName(taskDef.getTaskName())
                .action(ScheduleCommand.NAME)
                .sendDateUtc(LocalDateTime.now(clock))
                .body(bytes)
                .build();

        saveCommand(commandEntity, isImmediately);
        return taskId;
    }

    @Override
    public <T> void reschedule(TaskId taskId, Duration delay) throws Exception {
        executeTxAware(
                () -> reschedule(taskId, delay, false),
                false
        );
    }

    @Override
    public <T> void rescheduleImmediately(TaskId taskId, Duration delay) throws Exception {
        executeTxAware(
                () -> reschedule(taskId, delay, true),
                true
        );
    }

    private <T> void reschedule(TaskId taskId,
                                Duration delay,
                                boolean isImmediately) throws Exception {
        var command = RescheduleCommand.<T>builder()
                .taskId(taskId)
                .delay(delay)
                .build();
        byte[] bytes = taskSerializer.writeValue(command);
        var commandEntity = RemoteCommandEntity.builder()
                .appName(taskId.getAppName())
                .taskName(taskId.getTaskName())
                .action(RescheduleCommand.NAME)
                .sendDateUtc(LocalDateTime.now(clock))
                .body(bytes)
                .build();

        saveCommand(commandEntity, isImmediately);
    }

    @Override
    public <T> TaskId scheduleJoin(TaskDef<T> taskDef, ExecutionContext<T> executionContext, List<TaskId> joinList) throws Exception {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public <T> void setJoinMessageToBranch(JoinTaskMessage<T> joinTaskMessage) {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public <T> List<JoinTaskMessage<T>> getJoinMessagesFromBranch(TaskDef<T> taskDef) throws Exception {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public <T> void rescheduleByTaskDef(TaskDef<T> taskDef, Duration delay) throws Exception {
        executeTxAware(
                () -> rescheduleByTaskDef(taskDef, false),
                false
        );
    }

    @Override
    public <T> void rescheduleByTaskDefImmediately(TaskDef<T> taskDef, Duration delay) throws Exception {
        executeTxAware(
                () -> rescheduleByTaskDef(taskDef, true),
                true
        );
    }

    private <T> void rescheduleByTaskDef(TaskDef<T> taskDef, boolean isImmediately) throws IOException {
        var command = RescheduleByTaskDefCommand.<T>builder()
                .taskDef(taskDef)
                .build();
        byte[] bytes = taskSerializer.writeValue(command);
        RemoteCommandEntity commandEntity = RemoteCommandEntity.builder()
                .appName(taskDef.getAppName())
                .taskName(taskDef.getTaskName())
                .action(RescheduleByTaskDefCommand.NAME)
                .sendDateUtc(LocalDateTime.now(clock))
                .body(bytes)
                .build();
        saveCommand(commandEntity, isImmediately);
    }

    @Override
    public boolean cancelTaskExecution(TaskId taskId) throws Exception {
        return executeTxAware(
                () -> cancelTaskExecution(taskId, false),
                false
        );
    }

    @Override
    public boolean cancelTaskExecutionImmediately(TaskId taskId) throws Exception {
        return executeTxAware(
                () -> cancelTaskExecution(taskId, true),
                true
        );
    }

    private boolean cancelTaskExecution(TaskId taskId, boolean isImmediately) throws IOException {
        var command = CancelTaskCommand.builder()
                .taskId(taskId)
                .build();
        byte[] bytes = taskSerializer.writeValue(command);
        RemoteCommandEntity commandEntity = RemoteCommandEntity.builder()
                .appName(taskId.getAppName())
                .taskName(taskId.getTaskName())
                .action(CancelTaskCommand.NAME)
                .sendDateUtc(LocalDateTime.now(clock))
                .body(bytes)
                .build();
        saveCommand(commandEntity, isImmediately);
        return true;
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDef(TaskDef<T> taskDef) throws Exception {
        return executeTxAware(
                () -> cancelAllTaskByTaskDef(taskDef, false),
                false
        );
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDefImmediately(TaskDef<T> taskDef) throws Exception {
        return executeTxAware(
                () -> cancelAllTaskByTaskDef(taskDef, true),
                true
        );
    }

    private <T> boolean cancelAllTaskByTaskDef(TaskDef<T> taskDef, boolean isImmediately) throws IOException {
        var command = CancelTaskByTaskDefCommand.<T>builder()
                .taskDef(taskDef)
                .build();
        byte[] bytes = taskSerializer.writeValue(command);
        RemoteCommandEntity commandEntity = RemoteCommandEntity.builder()
                .appName(taskDef.getAppName())
                .taskName(taskDef.getTaskName())
                .action(CancelTaskByTaskDefCommand.NAME)
                .sendDateUtc(LocalDateTime.now(clock))
                .body(bytes)
                .build();
        saveCommand(commandEntity, isImmediately);
        return true;
    }

    @Override
    public boolean cancelWorkflow(UUID workflowId) {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public boolean cancelWorkflowImmediately(UUID workflowId) {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public void waitCompletion(TaskId taskId) throws TimeoutException {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public void waitCompletion(UUID workflowId) throws TimeoutException {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public <T> boolean isOwnTask(TaskDef<T> taskDef) {
        return taskRegistryService.getRegisteredTask(taskDef)
                .map(RegisteredTask::getTask)
                .map(task -> task.getClass().equals(RemoteStubTask.class))
                .orElse(false);
    }

    @Override
    public boolean isOwnTask(TaskId taskId) {
        TaskDef<Void> taskDef = TaskDef.publicTaskDef(taskId.getAppName(), taskId.getTaskName(), Void.class);
        return isOwnTask(taskDef);
    }

    private void saveCommand(RemoteCommandEntity commandEntity, boolean isImmediately) {
        Optional<WorkerContext> currentContext = workerContextManager.getCurrentContext();
        boolean isInContext = currentContext.isPresent();
        if (isInContext && !isImmediately) {
            currentContext.get().getRemoteCommandsToSend().add(commandEntity);
            log.info("saveCommand(): postponed command=[{}]", commandEntity);
        } else {
            remoteCommandRepository.save(commandEntity);
            log.info("saveCommand(): command=[{}]", commandEntity);
        }
    }
}
