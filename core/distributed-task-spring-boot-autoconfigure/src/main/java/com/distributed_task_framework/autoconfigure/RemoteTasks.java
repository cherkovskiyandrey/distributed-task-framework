package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.model.TaskDef;

import java.util.Collection;
import java.util.List;

/**
 * Interface to registry remote tasks from code.
 */
public interface RemoteTasks {

    default Collection<TaskDef<?>> remoteTasks() {
        return List.of();
    }

    default Collection<RemoteTaskWithParameters<?>> remoteTasksWithSettings() {
        return List.of();
    }
}
