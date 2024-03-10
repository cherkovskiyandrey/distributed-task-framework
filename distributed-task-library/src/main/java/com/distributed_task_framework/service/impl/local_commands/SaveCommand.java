package com.distributed_task_framework.service.impl.local_commands;

import com.distributed_task_framework.model.TaskId;
import com.google.common.collect.ImmutableList;
import lombok.Value;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.BatcheableLocalCommand;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;

import javax.annotation.Nullable;
import java.util.List;

@Value(staticConstructor = "of")
public class SaveCommand implements BatcheableLocalCommand<BatchSaveCommand> {
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
    public Class<BatchSaveCommand> batchClass() {
        return BatchSaveCommand.class;
    }

    @Override
    public BatchSaveCommand addToBatch(@Nullable BatchSaveCommand batch) {
        if (batch == null) {
            return BatchSaveCommand.of(List.of(taskEntity));
        }
        return BatchSaveCommand.of(ImmutableList.<TaskEntity>builder()
                .addAll(batch.getTaskEntities())
                .add(taskEntity)
                .build()
        );
    }
}
