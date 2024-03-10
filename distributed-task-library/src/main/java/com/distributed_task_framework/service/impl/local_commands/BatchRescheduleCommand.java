package com.distributed_task_framework.service.impl.local_commands;

import com.distributed_task_framework.model.TaskId;
import lombok.Value;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.LocalCommand;

import java.util.Collection;
import java.util.Objects;

@Value(staticConstructor = "of")
public class BatchRescheduleCommand implements LocalCommand {
    Collection<TaskEntity> taskEntities;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.rescheduleAll(taskEntities);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return taskEntities.stream().anyMatch(taskEntity -> Objects.equals(taskEntity.getId(), taskId.getId()));
    }
}
