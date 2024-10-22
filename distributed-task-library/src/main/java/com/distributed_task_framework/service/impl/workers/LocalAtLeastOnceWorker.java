package com.distributed_task_framework.service.impl.workers;

import com.distributed_task_framework.exception.BatchUpdateException;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.FailedExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessageContainer;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.model.WorkerContext;
import com.distributed_task_framework.persistence.entity.DltEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.repository.DltRepository;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.impl.CronService;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.utils.CommandHelper;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Slf4j
@FieldDefaults(level = AccessLevel.PROTECTED, makeFinal = true)
@RequiredArgsConstructor
public class LocalAtLeastOnceWorker implements TaskWorker {
    private static final List<Tag> COMMON_TAGS = List.of(Tag.of("name", "atLeastOnce"));

    ClusterProvider clusterProvider;
    WorkerContextManager workerContextManager;
    PlatformTransactionManager transactionManager;
    InternalTaskCommandService internalTaskCommandService;
    TaskRepository taskRepository;
    RemoteCommandRepository remoteCommandRepository;
    DltRepository dltRepository;
    TaskSerializer taskSerializer;
    CronService cronService;
    TaskMapper taskMapper;
    CommonSettings commonSettings;
    TaskLinkManager taskLinkManager;
    MetricHelper metricHelper;
    Clock clock;

    protected List<Tag> getCommonTags() {
        return COMMON_TAGS;
    }

    @Override
    public boolean isApplicable(TaskEntity taskEntity, TaskSettings taskParameters) {
        return TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE.equals(taskParameters.getExecutionGuarantees());
    }

    @Override
    public <T> void execute(TaskEntity taskEntity, RegisteredTask<T> registeredTask) {

        //todo: log which thread begin processing

        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        Task<T> task = registeredTask.getTask();
        var taskSettings = registeredTask.getTaskSettings();
        workerContextManager.setCurrentContext(WorkerContext.builder()
            .taskSettings(taskSettings)
            .workflowId(taskEntity.getWorkflowId())
            .currentTaskId(taskId)
            .taskEntity(taskEntity)
            .build()
        );

        final TaskEntity finalTaskEntity = taskEntity;
        try {
            getRunInternalTimer(finalTaskEntity).record(() -> runInternal(finalTaskEntity, registeredTask));
        } catch (Exception exception) {
            //think about recurrent tasks and there cancellation
            //think about not to cancel expired tasks, because they can be retried successfully
            //think about interrupting of canceled tasks from WorkerManagerImpl
            //add ability to handle unrecoverable errors
            boolean isCanceled = handleTaskCancellation(taskId, finalTaskEntity, taskSettings);
            if (isCanceled) {
                return;
            }

            boolean isChangedConcurrently = handleConcurrentChanges(finalTaskEntity, exception);
            if (isChangedConcurrently) {
                return;
            }

            getCommonErrorCounter(taskEntity).increment();
            int currentFailures = taskEntity.getFailures() + 1;
            taskEntity = taskEntity.toBuilder()
                .failures(currentFailures)
                .build();
            Optional<LocalDateTime> nextTryDateTime = taskSettings.getRetry().nextRetry(currentFailures, clock);

            boolean hasToInterruptRetrying = false;
            try {
                Optional<T> inputMessage = deserializeMessageSilent(
                    taskId,
                    taskEntity.getMessageBytes(),
                    task.getDef().getInputMessageType(),
                    taskEntity
                );
                List<T> inputJoinTaskMessages = readJoinTaskMessagesSilent(taskEntity, task.getDef().getInputMessageType());
                workerContextManager.resetCurrentContext();

                var failedExecutionContext = FailedExecutionContext.<T>builder()
                    .workflowId(taskEntity.getWorkflowId())
                    .currentTaskId(taskId)
                    .inputMessage(inputMessage.orElse(null))
                    .inputJoinTaskMessages(inputJoinTaskMessages)
                    .error(exception)
                    .failures(currentFailures)
                    .isLastAttempt(nextTryDateTime.isEmpty())
                    .executionAttempt(currentFailures)
                    .build();

                log.error(
                    "execute(): before onFailureWithResult for task=[{}]: attempts=[{}], isLastAttempt=[{}]",
                    taskId,
                    failedExecutionContext.getFailures(),
                    failedExecutionContext.isLastAttempt(),
                    failedExecutionContext.getError()
                );

                hasToInterruptRetrying = task.onFailureWithResult(failedExecutionContext);
            } catch (Exception innerException) {
                workerContextManager.resetCurrentContext();
                getEngineErrorCounter(taskEntity).increment();
                log.error("execute(): error during execution of Task#onFailed handler for task=[{}]", taskId, innerException);
            }

            boolean isCanceledInOnFailure = handleTaskCancellation(taskId, taskEntity, taskSettings);
            if (isCanceledInOnFailure) {
                return;
            }

            boolean isTheLastAttemptForGeneralTask = !taskSettings.hasCron() &&
                (hasToInterruptRetrying || nextTryDateTime.isEmpty());

            if (isTheLastAttemptForGeneralTask) {
                log.warn("execute(): failed task=[{}] {}", taskId, reasonMessage(hasToInterruptRetrying));
                finalizeFailedTaskSilently(taskEntity, taskSettings);
                return;
            }

            boolean isTheLastAttemptForCronTask = taskSettings.hasCron() &&
                (hasToInterruptRetrying || nextTryDateTime.isEmpty());
            if (isTheLastAttemptForCronTask) {
                log.warn("execute(): failed recurrent task=[{}] {}, will be rescheduled", taskId, reasonMessage(hasToInterruptRetrying));
                nextTryDateTime = cronService.nextExecutionDate(taskSettings.getCron(), false);
                if (nextTryDateTime.isEmpty()) {
                    log.warn("execute(): failed recurrent task=[%s] finished".formatted(taskId));
                    finalizeFailedTaskSilently(taskEntity, taskSettings);
                    return;
                }
            }

            try {
                taskEntity = taskEntity.toBuilder()
                    .assignedWorker(null)
                    .executionDateUtc(nextTryDateTime.orElseThrow())
                    .build();
                internalTaskCommandService.forceReschedule(taskEntity);
            } catch (Exception internalException) {
                logTaskUpdateException(finalTaskEntity, internalException);
            }
        } catch (Throwable throwable) {
            getEngineErrorCounter(finalTaskEntity).increment();
            log.error("execute(): fatal task error for taskId=[{}]", taskId, throwable);
            throw throwable;
        } finally {
            workerContextManager.cleanCurrentContext();
            log.info("execute(): completed taskId=[{}]", taskId);
        }
    }

    private String reasonMessage(boolean hasToInterruptRetrying) {
        return hasToInterruptRetrying ? "has been interrupted to retry" : "exceed all attempts";
    }

    private void finalizeFailedTaskSilently(TaskEntity taskEntity, TaskSettings taskSettings) {
        try {
            new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                WorkerContext currentContext = workerContextManager.getCurrentContext()
                    .orElseThrow(() -> new IllegalStateException("Context hasn't been set"));

                handlePostponedCommands(currentContext);
                handleLinks(currentContext);

                if (taskSettings.isDltEnabled()) {
                    DltEntity dltEntity = taskMapper.mapToDlt(taskEntity);
                    dltRepository.save(dltEntity);
                }

                if (!currentContext.isProcessedByLocalCommand()) {
                    internalTaskCommandService.finalize(taskEntity);
                    getFinalCompletedTaskWithErrorCounter(taskEntity).increment();
                }
            });
        } catch (Exception internalException) {
            logTaskUpdateException(taskEntity, internalException);
        }
    }

    private boolean handleConcurrentChanges(TaskEntity taskEntity, Throwable throwable) {
        return handleOptLockException(taskEntity, throwable)
            || handleBatchUpdateException(taskEntity, throwable);
    }

    private boolean handleOptLockException(TaskEntity taskEntity, Throwable throwable) {
        OptimisticLockException optimisticLockException = ExceptionUtils.throwableOfType(
            throwable,
            OptimisticLockException.class
        );

        // 1. protection from parallel execution - it isn't error of business logic of task
        // 2. has been rescheduled - will be taken by planner next time
        if (optimisticLockException != null && optimisticLockException.getEntityClass().equals(TaskEntity.class)) {
            getOptLockCounter(taskEntity).increment();
            log.warn("execute(): task=[{}] has been executed in parallel or canceled or rescheduled", taskEntity);
            return true;
        }
        return false;
    }

    private boolean handleBatchUpdateException(TaskEntity taskEntity, Throwable throwable) {
        BatchUpdateException batchUpdateException = ExceptionUtils.throwableOfType(
            throwable,
            BatchUpdateException.class
        );

        if (batchUpdateException != null && !batchUpdateException.getOptimisticLockTaskIds().isEmpty()) {
            getOptLockCounter(taskEntity).increment();
            log.warn(
                "execute(): tasks=[{}] has been executed in parallel or canceled or rescheduled",
                batchUpdateException.getOptimisticLockTaskIds()
            );
            return true;
        }
        return false;
    }

    private boolean handleTaskCancellation(TaskId taskId, TaskEntity taskEntity, TaskSettings taskSettings) {
        Optional<TaskEntity> updatedTaskEntityOpt = taskRepository.find(taskId.getId());
        if (updatedTaskEntityOpt.isEmpty()) {
            log.error("handleTaskCancellation(): failed task=[{}] has been removed unexpectedly!!!", taskId);
            getUnexpectedDeletingErrorCounter(taskEntity).increment();
            return true;
        }

        TaskEntity finalTaskEntity = updatedTaskEntityOpt.get();
        if (finalTaskEntity.isCanceled()) {
            if (taskSettings.hasCron()) {
                log.warn("handleTaskCancellation(): recurrent taskId=[{}] has been interrupted and canceled", taskId);
            } else {
                log.warn("handleTaskCancellation(): taskId=[{}] has been interrupted and canceled", taskId);
            }
            try {
                new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                    taskLinkManager.markLinksAsCompleted(finalTaskEntity.getId());
                    internalTaskCommandService.finalize(taskEntity);
                    getCancelCounter(finalTaskEntity).increment();
                });
            } catch (Exception internalException) {
                logTaskUpdateException(finalTaskEntity, internalException);
            }
            return true;
        }
        return false;
    }

    private void logTaskUpdateException(TaskEntity taskEntity, Throwable throwable) {
        boolean isConcurrentChanges = handleConcurrentChanges(taskEntity, throwable);
        if (isConcurrentChanges) {
            return;
        }
        getEngineErrorCounter(taskEntity).increment();
        log.error("execute(): can't update task=[{}]", taskEntity, throwable);
    }

    /**
     * @noinspection unchecked
     */
    private <T> Optional<T> deserializeMessageSilent(TaskId taskId,
                                                     @Nullable byte[] messageBytes,
                                                     JavaType inputMessageClass,
                                                     TaskEntity finalTaskEntity) {
        if (messageBytes == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(taskSerializer.readValue(messageBytes, inputMessageClass));
        } catch (Exception e) {
            getEngineErrorCounter(finalTaskEntity).increment();
            log.warn("deserializeMessageSilent(): can't read message for taskId=[{}]", taskId, e);
            return Optional.empty();
        }
    }

    private <T> List<T> readJoinTaskMessagesSilent(TaskEntity taskEntity, JavaType inputMessageType) {
        try {
            return readJoinTaskMessages(taskEntity, inputMessageType);
        } catch (Exception e) {
            log.warn("readJoinTaskMessagesSilent(): can't read message for taskId=[{}]", taskEntity.getId(), e);
            return List.of();
        }
    }

    @SneakyThrows
    protected <T> void runInternal(final TaskEntity taskEntity, RegisteredTask<T> registeredTask) {
        TaskId taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
        var taskSettings = registeredTask.getTaskSettings();
        Task<T> task = registeredTask.getTask();

        if (taskEntity.isCanceled()) {
            log.info("runInternal(): task=[{}] has been canceled, ignoring and finalize it", taskId);
        } else {
            T inputMessage = taskEntity.getMessageBytes() != null ?
                taskSerializer.readValue(taskEntity.getMessageBytes(), task.getDef().getInputMessageType()) :
                null;
            List<T> inputJoinTaskMessages = readJoinTaskMessages(taskEntity, task.getDef().getInputMessageType());

            ExecutionContext<T> executionContext = ExecutionContext.<T>builder()
                .workflowId(taskEntity.getWorkflowId())
                .workflowCreatedDateUtc(taskEntity.getWorkflowCreatedDateUtc())
                .affinity(taskEntity.getAffinity())
                .affinityGroup(taskEntity.getAffinityGroup())
                .currentTaskId(taskId)
                .inputMessage(inputMessage)
                .inputJoinTaskMessages(inputJoinTaskMessages)
                .executionAttempt(taskEntity.getFailures() + 1)
                .build();

            task.execute(executionContext);
        }

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.executeWithoutResult(status -> {
            WorkerContext currentContext = workerContextManager.getCurrentContext()
                .orElseThrow(() -> new IllegalStateException("Context hasn't been set"));

            handlePostponedCommands(currentContext);
            handleLinks(currentContext);
            if (!currentContext.isProcessedByLocalCommand()) {
                finalizeSucceededTask(taskId, taskEntity, taskSettings);
                getFinalCompletedTaskCounter(taskEntity).increment();
            }
        });
    }

    /**
     * ----- links -----
     * 1. Detect leaves in current subtree
     * 2. Create link from all current leaf's to each output of current task
     * 3. Get output links for current task and remove them
     * <p>
     * ----- messages ----
     * 1. Do nothing. Gonna to save all messages untouched during execution of tasks in branch
     */
    private void handleLinks(WorkerContext currentContext) {
        TaskId taskId = currentContext.getCurrentTaskId();
        UUID currentTaskId = taskId.getId();
        if (!taskLinkManager.hasLinks(currentTaskId)) {
            log.debug("handleLinks(): taskId=[{}] doesn't have links to join tasks", taskId);
            return;
        }

        if (!currentContext.hasNewLocalChildren()) {
            log.debug("handleLinks(): taskId=[{}] doesn't have children, marked links as completed", taskId);
            taskLinkManager.markLinksAsCompleted(currentTaskId);
            return;
        }

        Set<UUID> childrenIds = currentContext.getAllChildrenIds();
        childrenIds = Sets.newHashSet(Sets.difference(childrenIds, currentContext.getDropJoinTasksIds()));
        if (childrenIds.isEmpty()) {
            log.debug("handleLinks(): taskId=[{}] has only dropJoin children, marked links as completed", taskId);
            taskLinkManager.markLinksAsCompleted(currentTaskId);
            return;
        }

        Set<UUID> leaves = taskLinkManager.detectLeaves(childrenIds);
        taskLinkManager.inheritLinks(currentTaskId, leaves);
        taskLinkManager.removeLinks(currentTaskId);
        log.info("handleLinks(): taskId=[{}] inherit links to children=[{}]", taskId, leaves);
    }

    @SneakyThrows
    private <T> List<T> readJoinTaskMessages(TaskEntity taskEntity, JavaType inputMessageType) {
        if (taskEntity.getJoinMessageBytes() == null) {
            return List.of();
        }
        JoinTaskMessageContainer joinTaskMessageContainer = taskSerializer.readValue(
            taskEntity.getJoinMessageBytes(),
            JoinTaskMessageContainer.class
        );
        List<T> result = Lists.newArrayList();
        for (byte[] message : joinTaskMessageContainer.getRawMessages()) {
            result.add(taskSerializer.readValue(message, inputMessageType));
        }
        return result;
    }

    @SneakyThrows
    private void handlePostponedCommands(WorkerContext currentContext) {
        CommandHelper.collapseToBatchedCommands(currentContext.getLocalCommands())
            .forEach(command -> command.execute(internalTaskCommandService));
        remoteCommandRepository.saveAll(currentContext.getRemoteCommandsToSend());
        for (var joinTaskMessage : currentContext.getTaskMessagesToSave().values()) {
            taskLinkManager.setJoinMessages(
                currentContext.getCurrentTaskId(),
                joinTaskMessage
            );
        }
    }

    private void finalizeSucceededTask(TaskId taskId, TaskEntity taskEntity, TaskSettings taskSettings) {
        if (taskSettings.hasCron()) {
            if (taskEntity.isCanceled()) {
                log.warn("runInternal(): recurrent task=[%s] has been canceled".formatted(taskId));
                internalTaskCommandService.finalize(taskEntity);
                getCancelCounter(taskEntity).increment();
                return;
            }
            Optional<String> cronOpt = Optional.ofNullable(taskSettings.getCron());
            Optional<LocalDateTime> nextExecutionDate = cronOpt.flatMap(cron -> cronService.nextExecutionDate(
                cron,
                false
            ));

            if (nextExecutionDate.isEmpty()) {
                log.warn("runInternal(): recurrent task=[%s] finished".formatted(taskId));
                internalTaskCommandService.finalize(taskEntity);
                return;
            }
            TaskEntity updatedTaskEntity = taskEntity.toBuilder()
                .assignedWorker(null)
                .failures(0)
                .executionDateUtc(nextExecutionDate.get())
                .build();
            internalTaskCommandService.schedule(updatedTaskEntity);
            return;
        }
        internalTaskCommandService.finalize(taskEntity);
    }

    private Timer getRunInternalTimer(TaskEntity taskEntity) {
        return metricHelper.timer(
            List.of("worker", "run", "timer"),
            getCommonTags(),
            taskEntity
        );
    }

    private Counter getCancelCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
            List.of("worker", "cancel"),
            getCommonTags(),
            taskEntity
        );
    }

    private Counter getOptLockCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
            List.of("worker", "optLock", "error"),
            getCommonTags(),
            taskEntity
        );
    }

    private Counter getCommonErrorCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
            List.of("worker", "common", "error"),
            getCommonTags(),
            taskEntity
        );
    }

    private Counter getEngineErrorCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
            List.of("worker", "engine", "error"),
            getCommonTags(),
            taskEntity
        );
    }

    private Counter getUnexpectedDeletingErrorCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
            List.of("worker", "unexpectedDeleting", "error"),
            getCommonTags(),
            taskEntity
        );
    }

    private Counter getFinalCompletedTaskCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
            List.of("worker", "final", "completed"),
            getCommonTags(),
            taskEntity
        );
    }

    private Counter getFinalCompletedTaskWithErrorCounter(TaskEntity taskEntity) {
        return metricHelper.counter(
            List.of("worker", "final", "completed", "error"),
            getCommonTags(),
            taskEntity
        );
    }
}
