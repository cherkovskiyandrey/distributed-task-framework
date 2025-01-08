package com.distributed_task_framework.local_commands;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;


public interface LocalCommand {

    void execute(InternalTaskCommandService internalTaskCommandService);

    boolean hasTask(TaskId taskId);
}
