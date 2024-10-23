package com.distributed_task_framework.service.impl.local_commands;

import com.distributed_task_framework.model.TaskId;
import lombok.Value;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.LocalCommand;

@Value(staticConstructor = "of")
public class FinalizeCommand implements LocalCommand {
    TaskEntity taskEntity;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.finalize(taskEntity);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return this.taskEntity.getId().equals(taskId.getId());
    }
}
