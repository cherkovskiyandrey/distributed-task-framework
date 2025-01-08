package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.LocalCommand;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;

import java.util.List;


public class BatchRescheduleCommand extends AbstractBatchTaskBasedCommand implements LocalCommand {

    public BatchRescheduleCommand(List<TaskEntity> taskEntities) {
        super(taskEntities);
    }

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.rescheduleAll(taskEntities);
    }
}
