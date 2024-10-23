package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.model.DltTask;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskCommandService;
import com.distributed_task_framework.service.internal.TaskCommandWithDetectorService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.utils.BiPredicateWithException;
import com.distributed_task_framework.utils.ConsumerWith2Exception;
import com.distributed_task_framework.utils.ConsumerWithException;
import com.distributed_task_framework.utils.FunctionWithException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class DistributedTaskServiceImpl implements DistributedTaskService {
    TaskRegistryService taskRegistryService;
    List<TaskCommandWithDetectorService> taskCommandServices;
    CommonSettings commonSettings;

    @Override
    public <T> void registerTask(Task<T> task) {
        taskRegistryService.registerTask(task, TaskSettings.DEFAULT);
    }

    @Override
    public <T> void registerTask(Task<T> task, TaskSettings taskSettings) {
        taskRegistryService.registerTask(task, taskSettings);
    }

    @Override
    public <T> void registerRemoteTask(TaskDef<T> taskDef, TaskSettings taskSettings) {
        taskRegistryService.registerRemoteTask(taskDef, taskSettings);
    }

    @Override
    public <T> void registerRemoteTask(TaskDef<T> taskDef) {
        taskRegistryService.registerRemoteTask(taskDef, TaskSettings.DEFAULT);
    }

    @Override
    public <T> Optional<RegisteredTask<T>> getRegisteredTask(String taskName) {
        return taskRegistryService.getRegisteredLocalTask(taskName);
    }

    @Override
    public <T> Optional<RegisteredTask<T>> getRegisteredTask(TaskDef<T> taskDef) {
        return taskRegistryService.getRegisteredTask(taskDef);
    }

    @Override
    public boolean isTaskRegistered(String taskName) {
        return taskRegistryService.isLocalTaskRegistered(taskName);
    }

    @Override
    public <T> boolean isTaskRegistered(TaskDef<T> taskDef) {
        return taskRegistryService.isTaskRegistered(taskDef);
    }

    @Override
    public boolean unregisterTask(String taskName) {
        return unregisterTask(TaskDef.publicTaskDef(
            commonSettings.getAppName(),
            taskName,
            Void.class
        ));
    }

    @Override
    public <T> boolean unregisterTask(TaskDef<T> taskDef) {
        if (taskRegistryService.unregisterTask(taskDef)) {
            for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
                if (taskCommandService.isOwnTask(taskDef)) {
                    try {
                        //In case of fails, eventually consistency is guaranteed by planner.
                        //It has to move tasks which can't be planned for a long time to DLT
                        taskCommandService.cancelAllTaskByTaskDef(taskDef);
                    } catch (Exception e) {
                        log.warn("unregisterTask(): can't cancel tasks by taskDef=[{}]", taskDef);
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.schedule(taskDef, executionContext)
        );
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.scheduleFork(taskDef, executionContext)
        );
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.scheduleImmediately(taskDef, executionContext)
        );
    }

    @Override
    public <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.schedule(taskDef, executionContext, delay)
        );
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.scheduleFork(taskDef, executionContext, delay)
        );
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.scheduleImmediately(taskDef, executionContext, delay)
        );
    }

    @Override
    public <T> TaskId scheduleJoin(TaskDef<T> taskDef, ExecutionContext<T> executionContext, List<TaskId> joinList) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.scheduleJoin(taskDef, executionContext, joinList)
        );
    }

    @Override
    public <T> List<JoinTaskMessage<T>> getJoinMessagesFromBranch(TaskDef<T> taskDef) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.getJoinMessagesFromBranch(taskDef)
        );
    }

    @Override
    public <T> void setJoinMessageToBranch(JoinTaskMessage<T> joinTaskMessage) {
        routeAndRun(
            joinTaskMessage.getTaskId(),
            (Consumer<TaskCommandService>) taskCommandService -> taskCommandService.setJoinMessageToBranch(joinTaskMessage)
        );
    }

    @Override
    public void reschedule(TaskId taskId, Duration delay) throws Exception {
        routeAndRun(
            taskId,
            (ConsumerWithException<TaskCommandService, Exception>)
                taskCommandService -> taskCommandService.reschedule(taskId, delay)
        );
    }

    @Override
    public void rescheduleImmediately(TaskId taskId, Duration delay) throws Exception {
        routeAndRun(
            taskId,
            (ConsumerWithException<TaskCommandService, Exception>)
                taskCommandService -> taskCommandService.rescheduleImmediately(taskId, delay)
        );
    }

    @Override
    public <T> void rescheduleByTaskDef(TaskDef<T> taskDef, Duration delay) throws Exception {
        routeAndRun(
            taskDef,
            taskCommandService -> taskCommandService.rescheduleByTaskDef(taskDef, delay)
        );
    }

    @Override
    public <T> void rescheduleByTaskDefImmediately(TaskDef<T> taskDef, Duration delay) throws Exception {
        routeAndRun(
            taskDef,
            taskCommandService -> taskCommandService.rescheduleByTaskDefImmediately(taskDef, delay)
        );
    }

    @Override
    public boolean cancelTaskExecution(TaskId taskId) throws Exception {
        return routeAndCall(
            taskId,
            taskCommandService -> taskCommandService.cancelTaskExecution(taskId)
        );
    }

    @Override
    public boolean cancelTaskExecutionImmediately(TaskId taskId) throws Exception {
        return routeAndCall(
            taskId,
            taskCommandService -> taskCommandService.cancelTaskExecutionImmediately(taskId)
        );
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDef(TaskDef<T> taskDef) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.cancelAllTaskByTaskDef(taskDef)
        );
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDefImmediately(TaskDef<T> taskDef) throws Exception {
        return routeAndCall(
            taskDef,
            taskCommandService -> taskCommandService.cancelAllTaskByTaskDefImmediately(taskDef)
        );
    }

    @Override
    public boolean cancelWorkflowByTaskId(TaskId taskId) throws Exception {
        return routeAndCall(
            taskId,
            taskCommandService -> taskCommandService.cancelWorkflowByTaskId(taskId)
        );
    }

    @Override
    public boolean cancelWorkflowByTaskIdImmediately(TaskId taskId) throws Exception {
        return routeAndCall(
            taskId,
            taskCommandService -> taskCommandService.cancelWorkflowByTaskIdImmediately(taskId)
        );
    }

    @Override
    public boolean cancelAllWorkflowByTaskId(List<TaskId> taskIds) throws Exception {
        return groupToRouteAndCall(
            taskIds,
            (taskCommandService, groupedTaskIds) -> cancelAllWorkflowByTaskId(groupedTaskIds)
        );
    }

    @Override
    public boolean cancelAllWorkflowByTaskIdImmediately(List<TaskId> taskIds) throws Exception {
        return groupToRouteAndCall(
            taskIds,
            (taskCommandService, groupedTaskIds) -> cancelAllWorkflowByTaskIdImmediately(groupedTaskIds)
        );
    }

    private boolean groupToRouteAndCall(List<TaskId> taskIds,
                                        BiPredicateWithException<TaskCommandWithDetectorService, List<TaskId>, Exception> action) throws Exception {
        Map<Optional<TaskCommandWithDetectorService>, List<TaskId>> serviceToTasks = taskIds.stream()
            .collect(Collectors.groupingBy(this::detectService));

        var unknownTaskIds = serviceToTasks.getOrDefault(Optional.<TaskCommandWithDetectorService>empty(), List.of());
        if (!unknownTaskIds.isEmpty()) {
            throw new UnknownTaskException(unknownTaskIds.get(0));
        }
        boolean result = true;
        for (var entry : serviceToTasks.entrySet()) {
            result &= action.test(entry.getKey().orElseThrow(), entry.getValue());
        }
        return result;
    }

    @Override
    public Collection<TaskDef<?>> getDltTaskDefs() {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public <T> Page<DltTask<?>> getDltTasksByDef(TaskDef<T> taskDef, Pageable pageable) {
        throw new UnsupportedOperationException("Isn't supported yet");
    }

    @Override
    public void rescheduleDltTasks(Collection<DltTask<?>> dltTasks) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void waitCompletion(TaskId taskId) throws TimeoutException, InterruptedException {
        routeAndRun(
            taskId,
            (ConsumerWith2Exception<TaskCommandService, TimeoutException, InterruptedException>)
                taskCommandService -> taskCommandService.waitCompletion(taskId)
        );
    }

    @Override
    public void waitCompletion(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException {
        routeAndRun(
            taskId,
            (ConsumerWith2Exception<TaskCommandService, TimeoutException, InterruptedException>)
                taskCommandService -> taskCommandService.waitCompletion(taskId, timeout)
        );
    }

    @Override
    public void waitCompletionAllWorkflow(TaskId taskId) throws TimeoutException, InterruptedException {
        routeAndRun(
            taskId,
            (ConsumerWith2Exception<TaskCommandService, TimeoutException, InterruptedException>)
                taskCommandService -> taskCommandService.waitCompletionAllWorkflow(taskId)
        );
    }

    @Override
    public void waitCompletionAllWorkflow(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException {
        routeAndRun(
            taskId,
            (ConsumerWith2Exception<TaskCommandService, TimeoutException, InterruptedException>)
                taskCommandService -> taskCommandService.waitCompletionAllWorkflow(taskId, timeout)
        );
    }

    private <T, E extends Throwable> void routeAndRun(TaskDef<T> taskDef,
                                                      ConsumerWithException<TaskCommandService, E> supplier) throws E {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                supplier.accept(taskCommandService);
                return;
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    private void routeAndRun(TaskId taskId,
                             Consumer<TaskCommandService> supplier) {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                supplier.accept(taskCommandService);
                return;
            }
        }
        throw new UnknownTaskException(taskId);
    }

    private <E extends Throwable> void routeAndRun(TaskId taskId,
                                                   ConsumerWithException<TaskCommandService, E> supplier) throws E {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                supplier.accept(taskCommandService);
                return;
            }
        }
        throw new UnknownTaskException(taskId);
    }

    private <E1 extends Throwable, E2 extends Throwable> void routeAndRun(TaskId taskId,
                                                                          ConsumerWith2Exception<TaskCommandService, E1, E2> supplier) throws E1, E2 {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                supplier.accept(taskCommandService);
                return;
            }
        }
        throw new UnknownTaskException(taskId);
    }

    private <T, R, E extends Throwable> R routeAndCall(TaskDef<T> taskDef,
                                                       FunctionWithException<R, TaskCommandService, E> callable) throws E {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return callable.call(taskCommandService);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    private <R, E extends Throwable> R routeAndCall(TaskId taskId,
                                                    FunctionWithException<R, TaskCommandService, E> callable) throws E {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                return callable.call(taskCommandService);
            }
        }
        throw new UnknownTaskException(taskId);
    }

    private Optional<TaskCommandWithDetectorService> detectService(TaskId taskId) {
        return taskCommandServices.stream()
            .filter(taskCommandService -> taskCommandService.isOwnTask(taskId))
            .findFirst();
    }
}
