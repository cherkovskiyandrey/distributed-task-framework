package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.JoinTaskExecution;
import com.distributed_task_framework.model.JoinTaskMessage;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface TaskLinkManager {

    /**
     * Create links between joinTask and tasks to join on appropriate level.
     * IMPORTANT: expected to be invoked from transaction
     *
     * @param joinList
     * @param joinTaskId
     */
    void createLinks(TaskId joinTaskId, List<TaskId> joinList);

    /**
     * Copy links from parent to child.
     *
     * @param parent
     * @param child
     */
    void inheritLinks(UUID parent, Set<UUID> children);

    /**
     * @param currentTaskId
     */
    void removeLinks(UUID currentTaskId);

    /**
     * Read current messages for join task.
     * Trow exception if unknown join task or name conflict.
     *
     * @param currentTaskId
     * @param joinTaskDef
     * @param <T>
     * @return messages if exists or empty messages but always for all relevant tasks
     */
    <T> Collection<JoinTaskMessage<T>> getJoinMessages(TaskId currentTaskId, TaskDef<T> joinTaskDef) throws IOException;

    /**
     * Rewrite messages of this branch for join task
     *
     * @param currentTaskId
     * @param joinTaskDef
     * @param messages
     * @param <T>
     */
    <T> void setJoinMessages(TaskId currentTaskId, JoinTaskMessage<T> joinTaskMessage) throws IOException;

    /**
     * Check whether task has join tasks.
     *
     * @param currentTaskId
     * @return
     */
    boolean hasLinks(UUID currentTaskId);

    /**
     * Mark links as completed
     *
     * @param currentTaskId
     * @return
     */
    void markLinksAsCompleted(UUID currentTaskId);

    /**
     * Return join tasks which dont have uncompleted branch of tasks to join
     * and only from the deepest level.
     */
    List<UUID> getReadyToPlanJoinTasks(int batch);

    /**
     * 1. Read trees
     * 2. Merge messages from all tasks for current joinTaskId and return
     * 3. For all tasks relevant to current joinTaskId climb to parent and on all level:
     * - merge messages for joinTask on level N
     * - create link current joinTask and joinTask on level N
     * <p>
     * IMPORTANT: expected to be invoked from transaction
     *
     * @param joinTaskExecutionIds
     * @return
     */
    List<JoinTaskExecution> prepareJoinTaskToPlan(List<UUID> joinTaskExecutionIds);

    /**
     * Detect leaves from trees.
     *
     * @param trees
     * @return
     */
    Set<UUID> detectLeaves(Set<UUID> trees);
}
