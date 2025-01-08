package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.local_commands.BatcheableLocalCommand;
import com.distributed_task_framework.local_commands.TaskBasedLocalCommand;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.google.common.collect.ImmutableList;
import lombok.Value;

import jakarta.annotation.Nullable;
import java.util.List;

@Value(staticConstructor = "of")
public class ScheduleCommand implements TaskBasedLocalCommand, BatcheableLocalCommand<BatchScheduleCommand> {
    TaskEntity taskEntity;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.schedule(taskEntity);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return taskEntity.getId().equals(taskId.getId());
    }

    @Override
    public Class<BatchScheduleCommand> batchClass() {
        return BatchScheduleCommand.class;
    }

    @Override
    public BatchScheduleCommand addToBatch(@Nullable BatchScheduleCommand batch) {
        if (batch == null) {
            return new BatchScheduleCommand(List.of(taskEntity));
        }
        return new BatchScheduleCommand(ImmutableList.<TaskEntity>builder()
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
