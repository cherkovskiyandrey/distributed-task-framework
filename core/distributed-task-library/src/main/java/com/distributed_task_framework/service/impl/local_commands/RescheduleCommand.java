package com.distributed_task_framework.service.impl.local_commands;

import com.distributed_task_framework.model.TaskId;
import com.google.common.collect.ImmutableList;
import lombok.Value;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.BatcheableLocalCommand;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;

import jakarta.annotation.Nullable;
import java.util.List;

@Value(staticConstructor = "of")
public class RescheduleCommand implements BatcheableLocalCommand<BatchRescheduleCommand> {
    TaskEntity taskEntity;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.reschedule(taskEntity);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return taskEntity.getId().equals(taskId.getId());
    }

    @Override
    public Class<BatchRescheduleCommand> batchClass() {
        return BatchRescheduleCommand.class;
    }

    @Override
    public BatchRescheduleCommand addToBatch(@Nullable BatchRescheduleCommand batch) {
        if (batch == null) {
            return BatchRescheduleCommand.of(List.of(taskEntity));
        }

        return BatchRescheduleCommand.of(ImmutableList.<TaskEntity>builder()
                .addAll(batch.getTaskEntities())
                .add(taskEntity)
                .build());
    }
}
