package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.BatcheableLocalCommand;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;

import java.util.List;

public class FinalizeCommand
    extends AbstractTaskBasedContextAwareCommand
    implements BatcheableLocalCommand<BatchFinalizeCommand> {

    public FinalizeCommand(TaskEntity taskEntity) {
        super(taskEntity);
    }

    @Override
    protected TaskEntity doExecute(InternalTaskCommandService internalTaskCommandService,
                                   TaskEntity basedTaskEntity) {
        return internalTaskCommandService.finalize(taskEntity);
    }

    @Override
    public Class<BatchFinalizeCommand> batchClass() {
        return BatchFinalizeCommand.class;
    }

    @Override
    public BatchFinalizeCommand addToBatch(@Nullable BatchFinalizeCommand batch) {
        if (batch == null) {
            return new BatchFinalizeCommand(List.of(taskEntity));
        }

        return new BatchFinalizeCommand(ImmutableList.<TaskEntity>builder()
            .addAll(batch.getTaskEntities())
            .add(taskEntity)
            .build()
        );
    }
}
