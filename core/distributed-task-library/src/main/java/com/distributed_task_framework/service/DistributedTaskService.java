package com.distributed_task_framework.service;

import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.task.Task;
import com.distributed_task_framework.settings.TaskSettings;

import java.util.Optional;

/**
 * Entry point service to manage tasks.
 */
public interface DistributedTaskService extends TaskCommandService, LocalDltTaskCommandService {

    /**
     * Registry task in the runtime with default parameters.
     *
     * @param task - new task to registry
     * @param <T>
     */
    <T> void registerTask(Task<T> task);

    /**
     * Registry task in the runtime with custom parameters.
     *
     * @param task         - new task to registry
     * @param <T>
     * @param taskSettings - parameters of task
     */
    <T> void registerTask(Task<T> task, TaskSettings taskSettings);

    /**
     * Registry remote task with default parameters.
     *
     * @param taskDef
     * @param <T>
     */
    <T> void registerRemoteTask(TaskDef<T> taskDef);

    /**
     * Registry remote task with custom parameters.
     *
     * @param taskDef
     * @param taskSettings
     * @param <T>
     */
    <T> void registerRemoteTask(TaskDef<T> taskDef, TaskSettings taskSettings);


    <T> Optional<RegisteredTask<T>> getRegisteredTask(String taskName);

    /**
     * Find task by task definition.
     *
     * @param taskDef
     * @param <T>
     * @return
     */
    <T> Optional<RegisteredTask<T>> getRegisteredTask(TaskDef<T> taskDef);

    /**
     * Check whether task is registered.
     * Deprecated and check ONLY local task by name.
     * Use {@link DistributedTaskService#isTaskRegistered(TaskDef)}
     *
     * @param taskName
     * @return
     */
    boolean isTaskRegistered(String taskName);

    /**
     * Check whether task is registered.
     *
     * @param taskDef
     * @param <T>
     * @return
     */
    <T> boolean isTaskRegistered(TaskDef<T> taskDef);

    /**
     * Unregister task by name.
     * Deprecated and check ONLY local task by name.
     * Use {@link DistributedTaskService#unregisterTask(TaskDef)}
     *
     * @param taskName
     * @return
     */
    boolean unregisterTask(String taskName);

    /**
     * Unregister task by taskDef.
     *
     * @param taskDef
     * @param <T>
     * @return
     */
    <T> boolean unregisterTask(TaskDef<T> taskDef);
}
