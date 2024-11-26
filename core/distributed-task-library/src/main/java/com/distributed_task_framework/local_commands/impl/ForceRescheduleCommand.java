package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.BatcheableLocalCommand;
import com.distributed_task_framework.local_commands.TaskBasedLocalCommand;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;
import lombok.Value;

import java.util.List;

@Value(staticConstructor = "of")
public class ForceRescheduleCommand implements TaskBasedLocalCommand, BatcheableLocalCommand<BatchForceRescheduleCommand> {
    TaskEntity taskEntity;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.forceReschedule(taskEntity);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return taskEntity.getId().equals(taskId.getId());
    }

    @Override
    public Class<BatchForceRescheduleCommand> batchClass() {
        return BatchForceRescheduleCommand.class;
    }

    @Override
    public BatchForceRescheduleCommand addToBatch(@Nullable BatchForceRescheduleCommand batch) {
        if (batch == null) {
            return new BatchForceRescheduleCommand(List.of(taskEntity));
        }

        return new BatchForceRescheduleCommand(ImmutableList.<TaskEntity>builder()
            .addAll(batch.getTaskEntities())
            .add(taskEntity)
            .build()
        );
    }

    @Override
    public TaskEntity taskEntity() {
        return taskEntity;
    }
}
