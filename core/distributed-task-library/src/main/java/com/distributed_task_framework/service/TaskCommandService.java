package com.distributed_task_framework.service;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

public interface TaskCommandService {

    /**
     * Schedule task to execute in the cluster according to taskParameters.
     * For recurrent task (cron in TaskParameters) - create new one or return existed TaskId.
     * Recurrent task has restriction: it is a singleton by taskNames.
     * When invoked from task itself real scheduling will be postponed until task completed.
     *
     * @param taskDef          - definition of task
     * @param executionContext - executionContext
     * @param <T>              - type of input message for task
     */
    <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception;


    /**
     * The same as {@link this#schedule(TaskDef, ExecutionContext)} but exclude itself from
     * join hierarchy of parent task.
     *
     * @param taskDef
     * @param executionContext
     * @param <T>
     * @return
     * @throws Exception
     */
    <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception;


    /**
     * The same as {@link this#schedule(TaskDef, ExecutionContext)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskDef
     * @param executionContext
     * @param <T>
     * @return
     * @throws Exception
     */
    <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext) throws Exception;

    /**
     * Schedule task to execute in the cluster according to taskParameters with delay.
     * For recurrent task (cron in TaskParameters) - throw exception.
     * When invoked from task itself real scheduling will be postponed until task completed.
     *
     * @param taskDef          - definition of task
     * @param executionContext - executionContext
     * @param delay            - delay
     * @param <T>              - type of input message for task
     * @return
     */
    <T> TaskId schedule(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception;

    /**
     * The same as {@link this#schedule(TaskDef, ExecutionContext, Duration)} but exclude itself from
     * join hierarchy of parent task.
     *
     * @param taskDef
     * @param executionContext
     * @param delay
     * @param <T>
     * @return
     * @throws Exception
     */
    <T> TaskId scheduleFork(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception;

    /**
     * The same as {@link this#schedule(TaskDef, ExecutionContext, Duration)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskDef
     * @param executionContext
     * @param delay
     * @param <T>
     * @return
     * @throws Exception
     */
    <T> TaskId scheduleImmediately(TaskDef<T> taskDef, ExecutionContext<T> executionContext, Duration delay) throws Exception;

    /**
     * Schedule join task.
     * Which will be invoked only after all workflows born by all tasks from joinList are completed.
     * Regardless how (full or partially with fails) they completed.
     * IMPORTANT: Works only or in transaction, or in scope of other task.
     * Scenario:
     * 1. schedule general tasks form task or transaction
     * 2. schedule join task and put tasksId of tasks to join to method
     * 3. optionally use from task {@link this#getJoinMessagesFromBranch(TaskDef)} and {@link this#setJoinMessageToBranch(JoinTaskMessage)}
     * in order to send you instance of message to join task bellow
     *
     * @param taskDef
     * @param executionContext
     * @param joinList
     * @param <T>
     * @return
     * @throws Exception
     */
    <T> TaskId scheduleJoin(TaskDef<T> taskDef, ExecutionContext<T> executionContext, List<TaskId> joinList) throws Exception;

    /**
     * Get list of current tasks which messages belong to destined for all join tasks on branch below.
     * Throw execution when this task doesn't link to join task defined by input parameter.
     *
     * @param taskDef
     * @param <T>
     * @return
     */
    <T> List<JoinTaskMessage<T>> getJoinMessagesFromBranch(TaskDef<T> taskDef) throws Exception;

    /**
     * Set and rewrite of current tasks belongs join messages destined for all join tasks on branch below.
     * Throw execution when this task doesn't link to join task defined by input parameter.
     *
     * @param joinTaskMessage messages
     * @param <T>
     * @throws Exception
     */
    <T> void setJoinMessageToBranch(JoinTaskMessage<T> joinTaskMessage);

    /**
     * Reschedule task by taskId to execute with delay.
     * When invoked from task itself real scheduling will be postponed until task is in progress.
     *
     * @param taskId
     * @param delay
     * @param <T>
     */
    void reschedule(TaskId taskId, Duration delay) throws Exception;

    /**
     * The same as {@link this#reschedule(TaskId, Duration)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskId
     * @param delay
     * @param <T>
     * @throws Exception
     */
    void rescheduleImmediately(TaskId taskId, Duration delay) throws Exception;

    /**
     * Reschedule all existed and not in the running state tasks to passed duration.
     * Useful method when task interacts with external service and service returns 429.
     * In this case we can postpone all interactions with service in order to give it a
     * time to provide free resources.
     * When invoked from task itself real scheduling will be postponed until task is in progress.
     *
     * @param taskDef - definition of task
     * @param delay
     */
    <T> void rescheduleByTaskDef(TaskDef<T> taskDef, Duration delay) throws Exception;

    /**
     * The same as {@link this#rescheduleByTaskDef(TaskDef, Duration)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskDef
     * @param delay
     * @param <T>
     */
    <T> void rescheduleByTaskDefImmediately(TaskDef<T> taskDef, Duration delay) throws Exception;

    /**
     * Cancel certain task execution by id.
     * Only if task hasn't been executed yet.
     * When invoked from task itself real scheduling will be postponed until task is in progress.
     *
     * @param taskId
     * @return
     */
    boolean cancelTaskExecution(TaskId taskId) throws Exception;

    /**
     * The same as {@link this#cancelTaskExecution(TaskId)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskId
     * @return
     * @throws IOException
     */
    boolean cancelTaskExecutionImmediately(TaskId taskId) throws Exception;

    /**
     * Cancel all tasks by taskDef.
     * When invoked from task itself real scheduling will be postponed until task is in progress.
     *
     * @param taskDef
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> boolean cancelAllTaskByTaskDef(TaskDef<T> taskDef) throws Exception;

    /**
     * The same as {@link this#cancelAllTaskByTaskDef(TaskDef)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskDef
     * @param <T>
     * @return
     * @throws IOException
     */
    <T> boolean cancelAllTaskByTaskDefImmediately(TaskDef<T> taskDef) throws Exception;

    /**
     * Cancel all current tasks related to workflow.
     * When invoked from task itself real scheduling will be postponed until task is in progress.
     *
     * @param taskId
     * @return
     */
    boolean cancelWorkflowByTaskId(TaskId taskId) throws Exception;

    /**
     * The same as {@link this#cancelWorkflowByTaskId(TaskId)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskId
     * @return
     */
    boolean cancelWorkflowByTaskIdImmediately(TaskId taskId) throws Exception;

    /**
     * The same as {@link this#cancelWorkflowByTaskId(TaskId)} but for batch.
     *
     * @param taskIds
     * @return
     */
    boolean cancelAllWorkflowsByTaskId(List<TaskId> taskIds) throws Exception;

    /**
     * The same as {@link this#cancelAllWorkflowsByTaskId(List)} but when invoked from
     * task then is implemented immediately without waiting for the end of current task.
     * Only one exception: current task.
     *
     * @param taskIds
     * @return
     */
    boolean cancelAllWorkflowsByTaskIdImmediately(List<TaskId> taskIds) throws Exception;

    /**
     * Wait until task referenced by taskId is completed.
     * Return immediately in case task is already completed.
     *
     * @param taskId
     * @throws java.util.concurrent.TimeoutException when default timeout is reached and task is still in progress
     */
    void waitCompletion(TaskId taskId) throws TimeoutException, InterruptedException;

    /**
     * Wait until task referenced by taskId is completed.
     * Return immediately in case task is already completed.
     *
     * @param taskId
     * @throws java.util.concurrent.TimeoutException when default timeout is reached and task is still in progress
     */
    void waitCompletion(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException;

    /**
     * Wait until workflow referenced by taskId is completed.
     * Return immediately in case all tasks are already completed.
     *
     * @param taskId
     * @throws java.util.concurrent.TimeoutException when default timeout is reached and task is still in progress
     */
    void waitCompletionAllWorkflow(TaskId taskId) throws TimeoutException, InterruptedException;

    /**
     * Wait until workflow referenced by taskId is completed.
     * Return immediately in case all tasks are already completed.
     *
     * @param taskId
     * @throws java.util.concurrent.TimeoutException when default timeout is reached and task is still in progress
     */
    void waitCompletionAllWorkflow(TaskId taskId, Duration timeout) throws TimeoutException, InterruptedException;

    /**
     * Wait until all workflows referenced by taskIds are completed.
     * Return immediately in case all tasks are already completed.
     *
     * @param taskIds
     * @throws java.util.concurrent.TimeoutException when default timeout is reached and task is still in progress
     */
    void waitCompletionAllWorkflows(Collection<TaskId> taskIds) throws TimeoutException, InterruptedException;

    /**
     * Wait until all workflows referenced by taskIds are completed.
     * Return immediately in case all tasks are already completed.
     *
     * @param taskIds
     * @throws java.util.concurrent.TimeoutException when default timeout is reached and task is still in progress
     */
    void waitCompletionAllWorkflows(Collection<TaskId> taskIds, Duration timeout) throws TimeoutException, InterruptedException;
}
