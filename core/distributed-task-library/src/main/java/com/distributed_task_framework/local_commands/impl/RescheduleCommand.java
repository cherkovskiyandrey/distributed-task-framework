package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.BatcheableLocalCommand;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nullable;
import lombok.ToString;

import java.util.List;


@ToString(callSuper = true)
public class RescheduleCommand
    extends AbstractTaskBasedContextAwareCommand
    implements BatcheableLocalCommand<BatchRescheduleCommand> {

    public RescheduleCommand(TaskEntity taskEntity) {
        super(taskEntity);
    }

    @Override
    protected TaskEntity doExecute(InternalTaskCommandService internalTaskCommandService,
                                   TaskEntity basedTaskEntity) {
        return internalTaskCommandService.reschedule(basedTaskEntity);
    }

    @Override
    public Class<BatchRescheduleCommand> batchClass() {
        return BatchRescheduleCommand.class;
    }

    @Override
    public BatchRescheduleCommand addToBatch(@Nullable BatchRescheduleCommand batch) {
        if (batch == null) {
            return new BatchRescheduleCommand(List.of(taskEntity));
        }

        return new BatchRescheduleCommand(ImmutableList.<TaskEntity>builder()
            .addAll(batch.getTaskEntities())
            .add(taskEntity)
            .build()
        );
    }
}
