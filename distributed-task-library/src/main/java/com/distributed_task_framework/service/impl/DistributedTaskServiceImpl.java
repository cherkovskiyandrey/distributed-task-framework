package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.DltTask;
import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.task.Task;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import com.distributed_task_framework.exception.UnknownTaskException;
import com.distributed_task_framework.service.internal.TaskCommandWithDetectorService;
import com.distributed_task_framework.service.internal.TaskRegistryService;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.TaskSettings;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.schedule(taskDef, executionContext);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.scheduleFork(taskDef, executionContext);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.scheduleImmediately(taskDef, executionContext);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.schedule(taskDef, executionContext, delay);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.scheduleFork(taskDef, executionContext, delay);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.scheduleImmediately(taskDef, executionContext, delay);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> TaskId scheduleJoin(TaskDef<T> taskDef, ExecutionContext<T> executionContext, List<TaskId> joinList) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.scheduleJoin(taskDef, executionContext, joinList);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> List<JoinTaskMessage<T>> getJoinMessagesFromBranch(TaskDef<T> taskDef) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.getJoinMessagesFromBranch(taskDef);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> void setJoinMessageToBranch(JoinTaskMessage<T> joinTaskMessage) {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(joinTaskMessage.getTaskId())) {
                taskCommandService.setJoinMessageToBranch(joinTaskMessage);
                return;
            }
        }
        throw new UnknownTaskException(joinTaskMessage.getTaskId());
    }

    @Override
    public <T> void reschedule(TaskId taskId, Duration delay) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                taskCommandService.reschedule(taskId, delay);
                return;
            }
        }
        throw new UnknownTaskException(taskId);
    }

    @Override
    public <T> void rescheduleImmediately(TaskId taskId, Duration delay) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                taskCommandService.rescheduleImmediately(taskId, delay);
                return;
            }
        }
        throw new UnknownTaskException(taskId);
    }

    @Override
    public <T> void rescheduleByTaskDef(TaskDef<T> taskDef, Duration delay) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                taskCommandService.rescheduleByTaskDef(taskDef, delay);
                return;
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> void rescheduleByTaskDefImmediately(TaskDef<T> taskDef, Duration delay) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                taskCommandService.rescheduleByTaskDefImmediately(taskDef, delay);
                return;
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public boolean cancelTaskExecution(TaskId taskId) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                return taskCommandService.cancelTaskExecution(taskId);
            }
        }
        throw new UnknownTaskException(taskId);
    }

    @Override
    public boolean cancelTaskExecutionImmediately(TaskId taskId) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskId)) {
                return taskCommandService.cancelTaskExecutionImmediately(taskId);
            }
        }
        throw new UnknownTaskException(taskId);
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDef(TaskDef<T> taskDef) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.cancelAllTaskByTaskDef(taskDef);
            }
        }
        throw new UnknownTaskException(taskDef);
    }

    @Override
    public <T> boolean cancelAllTaskByTaskDefImmediately(TaskDef<T> taskDef) throws Exception {
        for (TaskCommandWithDetectorService taskCommandService : taskCommandServices) {
            if (taskCommandService.isOwnTask(taskDef)) {
                return taskCommandService.cancelAllTaskByTaskDefImmediately(taskDef);
            }
        }
        throw new UnknownTaskException(taskDef);
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
}
