package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.LocalCommand;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import lombok.Getter;

import java.util.List;
import java.util.Objects;


@Getter
public abstract class AbstractBatchTaskBasedCommand implements LocalCommand {
    protected final List<TaskEntity> taskEntities;

    protected AbstractBatchTaskBasedCommand(List<TaskEntity> taskEntities) {
        this.taskEntities = taskEntities;
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return taskEntities.stream().anyMatch(taskEntity -> Objects.equals(taskEntity.getId(), taskId.getId()));
    }
}
