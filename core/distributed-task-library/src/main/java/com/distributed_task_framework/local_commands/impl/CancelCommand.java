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
public class CancelCommand implements TaskBasedLocalCommand, BatcheableLocalCommand<BatchCancelCommand> {
    TaskEntity taskEntity;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.cancel(taskEntity);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return this.taskEntity.getId().equals(taskId.getId());
    }

    @Override
    public Class<BatchCancelCommand> batchClass() {
        return BatchCancelCommand.class;
    }

    @Override
    public BatchCancelCommand addToBatch(@Nullable BatchCancelCommand batch) {
        if (batch == null) {
            return new BatchCancelCommand(List.of(taskEntity));
        }

        return new BatchCancelCommand(ImmutableList.<TaskEntity>builder()
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
