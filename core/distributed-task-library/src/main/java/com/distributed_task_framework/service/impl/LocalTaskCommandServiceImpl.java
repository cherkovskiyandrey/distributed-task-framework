package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.exception.CronExpiredException;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.local_commands.impl.CancelByTaskDefCommand;
import com.distributed_task_framework.local_commands.impl.CancelByWorkflowCommand;
import com.distributed_task_framework.local_commands.impl.CancelCommand;
import com.distributed_task_framework.local_commands.impl.CreateLinksCommand;
import com.distributed_task_framework.local_commands.impl.FinalizeCommand;
import com.distributed_task_framework.local_commands.impl.ForceRescheduleCommand;
import com.distributed_task_framework.local_commands.impl.RescheduleByTaskDefCommand;
import com.distributed_task_framework.local_commands.impl.RescheduleCommand;
import com.distributed_task_framework.local_commands.impl.ScheduleCommand;
import com.distributed_task_framework.mapper.TaskMapper;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.model.WorkerContext;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.distributed_task_framework.persistence.repository.TaskRepository;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.service.internal.CompletionService;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.TaskCommandWithDetectorService;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.service.internal.WorkerContextManager;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StringUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class LocalTaskCommandServiceImpl extends AbstractTaskCommandWithDetectorService implements TaskCommandWithDetectorService {
    TaskRepository taskRepository;
    TaskMapper taskMapper;
    TaskRegistryService taskRegistryService;
    TaskSerializer taskSerializer;
    CronService cronService;
    CommonSettings commonSettings;
    InternalTaskCommandService internalTaskCommandService;
    TaskLinkManager taskLinkManager;
    CompletionService completionService;
    Clock clock;

    public LocalTaskCommandServiceImpl(WorkerContextManager workerContextManager,
                                       PlatformTransactionManager transactionManager,
                                       TaskRepository taskRepository,
                                       TaskMapper taskMapper,
                                       TaskRegistryService taskRegistryService,
                                       TaskSerializer taskSerializer,
                                       CronService cronService,
                                       CommonSettings commonSettings,
                                       InternalTaskCommandService internalTaskCommandService,
                                       TaskLinkManager taskLinkManager,
                                       CompletionService completionService,
                                       Clock clock) {
        super(workerContextManager, transactionManager);
        this.taskRepository = taskRepository;
        this.taskMapper = taskMapper;
        this.taskRegistryService = taskRegistryService;
        this.taskSerializer = taskSerializer;
        this.cronService = cronService;
        this.commonSettings = commonSettings;
        this.internalTaskCommandService = internalTaskCommandService;
        this.taskLinkManager = taskLinkManager;
        this.completionService = completionService;
        this.clock = clock;
    }

    @Override
    public <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return scheduleBaseTxAware(
            taskDef,
            executionContext,
            Duration.ZERO,
            false,
            false
        );
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return scheduleBaseTxAware(
            taskDef,
            executionContext,
            Duration.ZERO,
            false,
            true
        );
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return scheduleBaseTxAware(
            taskDef,
            executionContext,
            Duration.ZERO,
            true,
            false
        );
    }

    @Override
    public <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return scheduleBaseTxAware(
            taskDef,
            executionContext,
            delay,
            false,
            false
        );
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return scheduleBaseTxAware(
            taskDef,
            executionContext,
            delay,
            false,
            true
        );
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return scheduleBaseTxAware(
            taskDef,
            executionContext,
            delay,
            true,
            false
        );
    }

    @Override
    public <T> TaskId scheduleJoin(TaskDef<T> taskDef, ExecutionContext<T> executionContext, List<TaskId> joinList) throws Exception {
        RegisteredTask<T> registeredTask = taskRegistryService.<T>getRegisteredLocalTask(taskDef.getTaskName())
            .orElseThrow(() -> new UnknownTaskException(taskDef));
        TaskSettings taskSettings = registeredTask.getTaskSettings();

        if (taskSettings.hasCron()) {
            throw new TaskConfigurationException("It is prohibited to schedule as joinTask cron task=[%s]".formatted(taskDef));
        }
        if (hasRemoteTasks(joinList)) {
            throw new TaskConfigurationException("It is prohibited to join foreign task, joinTaskDef=[%s], joinList=[%s]".formatted(
                taskDef, joinList
            ));
        }
        Optional<WorkerContext> currentContext = workerContextManager.getCurrentContext();
        if (hasCronTask(joinList, currentContext)) {
            throw new TaskConfigurationException("It is prohibited to join cron task, joinTaskDef=[%s], joinList=[%s]".formatted(
                taskDef, joinList
            ));
        }

        boolean isInContext = currentContext.isPresent();
        if (!isInContext && !TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("It is prohibited to schedule joinTask not from other task and " +
                "without transaction with tasks to join, joinTaskDef=[%s], joinList=[%s]".formatted(
                    taskDef, joinList
                ));
        }

        String taskName = taskDef.getTaskName();
        Optional<T> inputMessageOpt = executionContext.getInputMessageOpt();
        byte[] messageBytes = taskSerializer.writeValue(inputMessageOpt.orElse(null));
        LocalDateTime now = LocalDateTime.now(clock);

        TaskEntity taskEntity = TaskEntity.builder()
            .workflowId(executionContext.getWorkflowId())
            .workflowCreatedDateUtc(
                Optional.ofNullable(executionContext.getWorkflowCreatedDateUtc())
                    .orElse(now)
            )
            .affinityGroup(executionContext.getAffinityGroup())
            .affinity(executionContext.getAffinity())
            .taskName(taskName)
            .virtualQueue(VirtualQueue.NEW)
            .messageBytes(messageBytes)
            .executionDateUtc(now)
            .singleton(false)
            .notToPlan(true)
            .failures(0)
            .build();

        final TaskId taskId;
        if (isInContext) {
            WorkerContext workerContext = currentContext.get();
            taskEntity = taskEntity.toBuilder()
                .id(UUID.randomUUID()) //generate and bind uuid
                .createdDateUtc(now)
                .build();
            ScheduleCommand scheduleCommand = ScheduleCommand.of(taskEntity);
            workerContext.getLocalCommands().add(scheduleCommand);

            taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
            CreateLinksCommand createLinksCommand = CreateLinksCommand.builder()
                .joinTaskId(taskId)
                .joinList(joinList)
                .taskLinkManager(taskLinkManager)
                .build();
            workerContext.getLocalCommands().add(createLinksCommand);
            log.info("scheduleJoin(): postponed command=[{}]", scheduleCommand);

        } else {
            taskEntity = internalTaskCommandService.schedule(taskEntity);
            taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
            taskLinkManager.createLinks(taskId, joinList);
            log.info("scheduleJoin(): taskEntity=[{}]", taskEntity);
        }
        return taskId;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private boolean hasCronTask(List<TaskId> joinList, Optional<WorkerContext> currentContext) {
        Set<UUID> joinIdList = joinList.stream()
            .map(TaskId::getId)
            .collect(Collectors.toSet());
        Boolean hasCronInContext = currentContext.map(workerContext -> workerContext.hasCronTasksToSave(
                joinIdList,
                taskRegistryService
            )
        ).orElse(false);
        if (hasCronInContext) {
            return true;
        }

        List<TaskEntity> taskToJoinList = taskRepository.findAll(joinIdList);
        return taskToJoinList.stream().anyMatch(taskEntity -> taskRegistryService.getRegisteredLocalTask(taskEntity.getTaskName())
            .map(regTask -> regTask.getTaskSettings().hasCron())
            .orElse(false));
    }

    private boolean hasRemoteTasks(List<TaskId> joinList) {
        return joinList.stream().noneMatch(taskId -> commonSettings.getAppName().equals(taskId.getAppName()));
    }

    @Override
    public <T> List<JoinTaskMessage<T>> getJoinMessagesFromBranch(TaskDef<T> taskDef) throws Exception {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        boolean isInContext = currentContextOpt.isPresent();
        if (!isInContext) {
            throw new IllegalStateException("Method can be invoked only from scope of other task.");
        }
        WorkerContext workerContext = currentContextOpt.get();
        Collection<JoinTaskMessage<T>> joinMessagesFromContext = workerContext.getJoinMessagesFromBranch(taskDef);
        if (joinMessagesFromContext.isEmpty()) {
            Collection<JoinTaskMessage<T>> joinMessages = taskLinkManager.getJoinMessages(workerContext.getCurrentTaskId(), taskDef);
            workerContext.setJoinMessageToBranch(joinMessages);
        }
        return workerContext.getJoinMessagesFromBranch(taskDef);
    }

    @Override
    public <T> void setJoinMessageToBranch(JoinTaskMessage<T> joinTaskMessage) {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        boolean isInContext = currentContextOpt.isPresent();
        if (!isInContext) {
            throw new IllegalStateException("Method can be invoked only from scope of other task.");
        }
        WorkerContext workerContext = currentContextOpt.get();
        workerContext.replaceJoinMessageToBranch(List.of(joinTaskMessage));
    }

    private <T> TaskId scheduleBaseTxAware(TaskDef<T> taskDef,
                                           ExecutionContext<T> executionContext,
                                           Duration delay,
                                           boolean isImmediately,
                                           boolean hasToDropJoin) throws Exception {
        return executeTxAwareWithException(
            () -> scheduleBase(
                taskDef,
                executionContext,
                delay,
                isImmediately,
                hasToDropJoin
            ),
            isImmediately
        );
    }

    /**
     * @noinspection unchecked
     */
    private <T> TaskId scheduleBase(TaskDef<T> taskDef,
                                    ExecutionContext<T> executionContext,
                                    Duration delay,
                                    boolean isImmediately,
                                    boolean hasToDropJoin) throws Exception {
        RegisteredTask<T> registeredTask = taskRegistryService.<T>getRegisteredLocalTask(taskDef.getTaskName())
            .orElseThrow(() -> new UnknownTaskException(taskDef));
        String taskName = taskDef.getTaskName();

        //tasks natural has a cron - are singleton
        boolean isSingleton = registeredTask.getTaskSettings().hasCron();
        while (true) {
            if (isSingleton) {
                Collection<TaskEntity> singletonTask = taskRepository.findByName(taskName, 2);
                if (singletonTask.size() > 1 ||
                    singletonTask.size() == 1 && !singletonTask.iterator().next().isSingleton()) {
                    log.warn("scheduleBase(): singleton task=[{}] already has not-singleton instances", taskName);
                } else if (singletonTask.size() == 1) {
                    return taskMapper.map(singletonTask.iterator().next(), commonSettings.getAppName());
                }
            }

            Optional<T> inputMessageOpt = executionContext.getInputMessageOpt();
            byte[] messageBytes = taskSerializer.writeValue(inputMessageOpt.orElse(null));

            final LocalDateTime executionDateUtc;
            String cron = registeredTask.getTaskSettings().getCron();
            if (StringUtils.hasText(cron)) {
                executionDateUtc = cronService.nextExecutionDate(cron, true)
                    .orElseThrow(() -> new CronExpiredException(taskDef, cron))
                    .plus(delay);
            } else {
                executionDateUtc = LocalDateTime.now(clock).plus(delay);
            }

            TaskEntity taskEntity = TaskEntity.builder()
                .workflowId(executionContext.getWorkflowId())
                .workflowCreatedDateUtc(
                    Optional.ofNullable(executionContext.getWorkflowCreatedDateUtc())
                        .orElseGet(() -> LocalDateTime.now(clock))
                )
                .affinityGroup(executionContext.getAffinityGroup())
                .affinity(executionContext.getAffinity())
                .taskName(taskName)
                .virtualQueue(VirtualQueue.NEW)
                .messageBytes(messageBytes)
                .executionDateUtc(executionDateUtc)
                .singleton(isSingleton)
                .failures(0)
                .build();

            final TaskId taskId;
            Optional<WorkerContext> currentContext = workerContextManager.getCurrentContext();
            boolean isInContext = currentContext.isPresent();
            if (isInContext && !isImmediately) {
                WorkerContext workerContext = currentContext.get();
                taskEntity = taskEntity.toBuilder()
                    .id(UUID.randomUUID()) //generate and bind uuid
                    .createdDateUtc(LocalDateTime.now(clock))
                    .build();
                ScheduleCommand scheduleCommand = ScheduleCommand.of(taskEntity);
                workerContext.getLocalCommands().add(scheduleCommand);
                if (hasToDropJoin) {
                    workerContext.getDropJoinTasksIds().add(taskEntity.getId());
                }
                log.info("scheduleBase(): postponed command=[{}]", scheduleCommand);
                taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
            } else {
                try {
                    taskEntity = internalTaskCommandService.schedule(taskEntity);
                    taskId = taskMapper.map(taskEntity, commonSettings.getAppName());
                    log.info("scheduleBase(): taskEntity=[{}]", taskEntity);
                } catch (OptimisticLockException optimisticLockException) {
                    log.warn("scheduleBase(): task=[{}] is singleton and is already scheduled", taskName);
                    continue;
                }
            }
            return taskId;
        }
    }

    @Override
    public void reschedule(TaskId taskId, Duration delay) throws Exception {
        rescheduleBaseTxAware(taskId, delay, false);
    }

    @Override
    public void rescheduleImmediately(TaskId taskId, Duration delay) throws Exception {
        rescheduleBaseTxAware(taskId, delay, true);
    }

    void rescheduleBaseTxAware(TaskId taskId,
                               Duration delay,
                               boolean isImmediately) throws Exception {
        executeTxAwareWithException(
            () -> rescheduleBase(
                taskId,
                delay,
                isImmediately
            ),
            isImmediately
        );
    }

    private <T> void rescheduleBase(TaskId taskId,
                                    Duration delay,
                                    boolean isImmediately) {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        Optional<TaskEntity> currentTaskEntityOpt = currentContextOpt
            .filter(workerContext -> workerContext.getCurrentTaskId().equals(taskId))
            .map(WorkerContext::getTaskEntity);
        if (currentTaskEntityOpt.isPresent()) {
            WorkerContext workerContext = currentContextOpt.get();
            var taskEntity = currentTaskEntityOpt.get();
            taskEntity = taskEntity.toBuilder()
                .executionDateUtc(taskEntity.getExecutionDateUtc().plus(delay))
                .build();
            RescheduleCommand rescheduleCommand = new RescheduleCommand(taskEntity);
            workerContext.getLocalCommands().add(rescheduleCommand);
            log.info("reschedule(): postponed current executing command=[{}]", rescheduleCommand);
            return;
        }

        TaskEntity taskEntity = taskRepository.find(taskId.getId())
            .orElseThrow(() -> new UnknownTaskException(taskId));
        taskEntity = taskEntity.toBuilder()
            .executionDateUtc(taskEntity.getExecutionDateUtc().plus(delay))
            .build();

        if (currentContextOpt.isPresent() && !isImmediately) {
            WorkerContext workerContext = currentContextOpt.get();
            ForceRescheduleCommand forceRescheduleCommand = ForceRescheduleCommand.of(taskEntity);
            workerContext.getLocalCommands().add(forceRescheduleCommand);
            log.info("reschedule(): postponed command=[{}]", forceRescheduleCommand);
        } else {
            internalTaskCommandService.forceReschedule(taskEntity);
            log.info("reschedule(): taskId=[{}]", taskId);
        }
    }

    @Override
    public <T> void rescheduleByTaskDef(TaskDef<T> taskDef, Duration delay) throws Exception {
        rescheduleByTaskDefBaseTxAware(taskDef, delay, false);
    }

    @Override
    public <T> void rescheduleByTaskDefImmediately(TaskDef<T> taskDef, Duration delay) throws Exception {
        rescheduleByTaskDefBaseTxAware(taskDef, delay, true);
    }

    <T> void rescheduleByTaskDefBaseTxAware(TaskDef<T> taskDef,
                                            Duration delay,
                                            boolean isImmediately) throws Exception {
        executeTxAwareWithException(
            () -> rescheduleByTaskDefBase(
                taskDef,
                delay,
                isImmediately
            ),
            isImmediately
        );
    }

    private <T> boolean rescheduleByTaskDefBase(TaskDef<T> taskDef,
                                                Duration delay,
                                                boolean isImmediately) {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        List<TaskId> excludes = Lists.newArrayList();

        boolean hasCurrentTask = currentContextOpt
            .map(WorkerContext::getCurrentTaskId)
            .map(taskId -> taskDef.equals(TaskDef.privateTaskDef(taskId.getTaskName())))
            .orElse(false);

        if (hasCurrentTask) {
            WorkerContext workerContext = currentContextOpt.orElseThrow();
            var currentTaskEntity = workerContext.getTaskEntity();
            var currentTaskId = workerContext.getCurrentTaskId();
            currentTaskEntity = currentTaskEntity.toBuilder()
                .assignedWorker(null)
                .lastAssignedDateUtc(null)
                .executionDateUtc(currentTaskEntity.getExecutionDateUtc().plus(delay))
                .build();
            var rescheduleCommand = new RescheduleCommand(currentTaskEntity);
            workerContext.getLocalCommands().add(rescheduleCommand);
            log.info("rescheduleByTaskDefBase(): postponed current task=[{}]", currentTaskEntity);
            excludes.add(currentTaskId);
        }

        boolean isInContext = currentContextOpt.isPresent();
        if (isInContext && !isImmediately) {
            WorkerContext workerContext = currentContextOpt.orElseThrow();
            var rescheduleByTaskDefCommand = RescheduleByTaskDefCommand.of(taskDef, delay, excludes);
            workerContext.getLocalCommands().add(rescheduleByTaskDefCommand);
            log.info("rescheduleByTaskDefBase(): postponed command=[{}]", rescheduleByTaskDefCommand);
            return true;
        }

        return internalTaskCommandService.forceRescheduleAll(taskDef, delay, excludes) > 0;
    }

    @Override
    public boolean cancelTaskExecution(TaskId taskId) {
        return cancelTaskExecutionBaseTxAware(taskId, false);
    }

    @Override
    public boolean cancelTaskExecutionImmediately(TaskId taskId) {
        return cancelTaskExecutionBaseTxAware(taskId, true);
    }

    private boolean cancelTaskExecutionBaseTxAware(TaskId taskId, boolean isImmediately) {
        return executeTxAware(
            () -> cancelTaskExecutionBase(taskId, isImmediately),
            isImmediately
        );
    }

    private boolean cancelTaskExecutionBase(TaskId taskId, boolean isImmediately) {
        Optional<TaskEntity> taskEntityOpt = taskRepository.find(taskId.getId());
        if (taskEntityOpt.isEmpty()) {
            log.info("cancelTaskExecution(): there isn't task by taskId=[{}]", taskId);
            return false;
        }
        var taskEntity = taskEntityOpt.get();
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        Optional<TaskEntity> currentTaskEntityOpt = currentContextOpt
            .filter(workerContext -> workerContext.getCurrentTaskId().equals(taskId))
            .map(WorkerContext::getTaskEntity);
        if (currentTaskEntityOpt.isPresent()) {
            WorkerContext workerContext = currentContextOpt.get();
            taskEntity = currentTaskEntityOpt.get();
            if (workerContext.isCurrentTaskCron()) {
                log.info("cancelTaskExecution(): cancel current cron task=[{}], doesn't affect current invocation", taskEntity);
            } else {
                log.info("cancelTaskExecution(): cancel current executing task=[{}] prevent to reschedule it", taskEntity);
            }
            FinalizeCommand finalizeCommand = new FinalizeCommand(taskEntity);
            workerContext.getLocalCommands().add(finalizeCommand);
            return true;
        }

        boolean isInContext = currentContextOpt.isPresent();
        if (isInContext && !isImmediately) {
            WorkerContext workerContext = currentContextOpt.get();
            CancelCommand cancelCommand = CancelCommand.of(taskEntity);
            workerContext.getLocalCommands().add(cancelCommand);
            log.info("cancelTaskExecution(): postponed command=[{}]", cancelCommand);
            return true;
        }
        internalTaskCommandService.cancel(taskEntity);
        log.info("cancelTaskExecution(): taskId=[{}]", taskId);
        return true;
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDef(TaskDef<T> taskDef) {
        return cancelAllTaskByTaskIdBaseTxAware(taskDef, false);
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDefImmediately(TaskDef<T> taskDef) {
        return cancelAllTaskByTaskIdBaseTxAware(taskDef, true);
    }

    private <T> boolean cancelAllTaskByTaskIdBaseTxAware(TaskDef<T> taskDef, boolean isImmediately) {
        return executeTxAware(
            () -> cancelAllTaskByTaskDefBase(taskDef, isImmediately),
            isImmediately
        );
    }

    private <T> boolean cancelAllTaskByTaskDefBase(TaskDef<T> taskDef, boolean isImmediately) {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        List<TaskId> excludes = Lists.newArrayList();

        boolean hasCurrentTask = currentContextOpt
            .map(WorkerContext::getCurrentTaskId)
            .map(taskId -> taskDef.equals(TaskDef.privateTaskDef(taskId.getTaskName())))
            .orElse(false);

        if (hasCurrentTask) {
            WorkerContext workerContext = currentContextOpt.orElseThrow();
            var currentTaskId = workerContext.getCurrentTaskId();
            if (workerContext.isCurrentTaskCron()) {
                log.info("cancelAllTaskByTaskDefBase(): cancel current cron task=[{}], doesn't affect current invocation", currentTaskId);
            } else {
                log.info("cancelAllTaskByTaskDefBase(): cancel current executing task=[{}] prevent to reschedule it", currentTaskId);
            }
            FinalizeCommand finalizeCommand = new FinalizeCommand(workerContext.getTaskEntity());
            workerContext.getLocalCommands().add(finalizeCommand);
            excludes.add(currentTaskId);
        }

        boolean isInContext = currentContextOpt.isPresent();
        if (isInContext && !isImmediately) {
            WorkerContext workerContext = currentContextOpt.orElseThrow();
            var cancelTaskByTaskIdCommand = CancelByTaskDefCommand.of(taskDef, excludes);
            workerContext.getLocalCommands().add(cancelTaskByTaskIdCommand);
            log.info("cancelAllTaskByTaskDefBase(): postponed command=[{}]", cancelTaskByTaskIdCommand);
            return true;
        }

        return internalTaskCommandService.cancelAll(taskDef, excludes) > 0;
    }

    @Override
    public boolean cancelWorkflowByTaskId(TaskId taskId) {
        return cancelAllWorkflowByTaskIdTxAware(List.of(taskId), false);
    }

    @Override
    public boolean cancelWorkflowByTaskIdImmediately(TaskId taskId) {
        return cancelAllWorkflowByTaskIdTxAware(List.of(taskId), true);
    }

    @Override
    public boolean cancelAllWorkflowsByTaskId(List<TaskId> taskIds) {
        return cancelAllWorkflowByTaskIdTxAware(taskIds, false);
    }

    @Override
    public boolean cancelAllWorkflowsByTaskIdImmediately(List<TaskId> taskIds) {
        return cancelAllWorkflowByTaskIdTxAware(taskIds, true);
    }

    private boolean cancelAllWorkflowByTaskIdTxAware(List<TaskId> taskIds, boolean isImmediately) {
        return executeTxAware(
            () -> cancelWorkflowByTaskIdBase(taskIds, isImmediately),
            isImmediately
        );
    }

    private boolean cancelWorkflowByTaskIdBase(Collection<TaskId> taskIds, boolean isImmediately) {
        Optional<WorkerContext> currentContextOpt = workerContextManager.getCurrentContext();
        List<TaskId> excludes = Lists.newArrayList();

        var workflows = taskIds.stream().map(TaskId::getWorkflowId).collect(Collectors.toSet());
        boolean hasCurrentTask = currentContextOpt
            .map(WorkerContext::getCurrentTaskId)
            .map(taskId -> workflows.contains(taskId.getWorkflowId()))
            .orElse(false);

        if (hasCurrentTask) {
            WorkerContext workerContext = currentContextOpt.orElseThrow();
            var currentTaskId = workerContext.getCurrentTaskId();
            if (workerContext.isCurrentTaskCron()) {
                log.info("cancelWorkflowByTaskIdBase(): cancel current cron task=[{}], doesn't affect current invocation", currentTaskId);
            } else {
                log.info("cancelWorkflowByTaskIdBase(): cancel current executing task=[{}] prevent to reschedule it", currentTaskId);
            }
            FinalizeCommand finalizeCommand = new FinalizeCommand(workerContext.getTaskEntity());
            workerContext.getLocalCommands().add(finalizeCommand);
            excludes.add(currentTaskId);
        }

        boolean isInContext = currentContextOpt.isPresent();
        if (isInContext && !isImmediately) {
            WorkerContext workerContext = currentContextOpt.orElseThrow();
            var cancelByWorkflowCommand = CancelByWorkflowCommand.of(workflows, excludes);
            workerContext.getLocalCommands().add(cancelByWorkflowCommand);
            log.info("cancelWorkflowByTaskIdBase(): postponed command=[{}]", cancelByWorkflowCommand);
            return true;
        }

        return internalTaskCommandService.cancelAll(workflows, excludes) > 0;
    }

    @Override
    public void waitCompletion(TaskId taskId) throws TimeoutException, InterruptedException {
        completionService.waitCompletion(taskId);
    }

    @Override
    public void waitCompletion(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException {
        completionService.waitCompletion(taskId, timeout);
    }

    @Override
    public void waitCompletionAllWorkflow(TaskId taskId) throws TimeoutException, InterruptedException {
        completionService.waitCompletionAllWorkflow(taskId.getWorkflowId());
    }

    @Override
    public void waitCompletionAllWorkflow(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException {
        completionService.waitCompletionAllWorkflow(taskId.getWorkflowId(), timeout);
    }

    @Override
    public void waitCompletionAllWorkflows(Collection<TaskId> taskIds) throws TimeoutException, InterruptedException {
        var workflows = taskIds.stream()
            .map(TaskId::getWorkflowId)
            .toList();
        completionService.waitCompletionAllWorkflows(workflows);
    }

    @Override
    public void waitCompletionAllWorkflows(Collection<TaskId> taskIds, Duration timeout) throws TimeoutException, InterruptedException {
        var workflows = taskIds.stream()
            .map(TaskId::getWorkflowId)
            .toList();
        completionService.waitCompletionAllWorkflows(workflows, timeout);
    }

    @Override
    public <T> boolean isOwnTask(TaskDef<T> taskDef) {
        return commonSettings.getAppName().equals(taskDef.getAppName()) ||
            !StringUtils.hasText(taskDef.getAppName());
    }

    @Override
    public boolean isOwnTask(TaskId taskId) {
        return commonSettings.getAppName().equals(taskId.getAppName()) ||
            !StringUtils.hasText(taskId.getAppName());
    }
}
