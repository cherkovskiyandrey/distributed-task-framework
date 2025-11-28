package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.task.Task;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface TaskRegistryService {

    <T> void registerTask(Task<T> task, TaskSettings taskSettings);

    <T> void registerRemoteTask(TaskDef<T> taskDef, TaskSettings taskSettings);

    Optional<TaskSettings> getLocalTaskParameters(String taskName);

    <T> Optional<TaskSettings> getTaskParameters(TaskDef<T> taskDef);

    boolean isLocalTaskRegistered(String taskName);

    <T> boolean isTaskRegistered(TaskDef<T> taskDef);

    boolean unregisterLocalTask(String taskName);

    <T> boolean unregisterTask(TaskDef<T> taskDef);

    <T> Optional<RegisteredTask<T>> getRegisteredLocalTask(String taskName);

    <T> Optional<TaskDef<T>> getRegisteredLocalTaskDef(String taskName);

    <T> Optional<RegisteredTask<T>> getRegisteredTask(TaskDef<T> taskDef);

    /**
     * Returned set of registered local tasks by node.
     *
     * @return
     */
    Map<UUID, Set<String>> getRegisteredLocalTaskInCluster();

    boolean hasClusterRegisteredTaskByName(String name);
}
