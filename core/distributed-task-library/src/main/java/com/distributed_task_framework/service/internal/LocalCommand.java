package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.TaskId;


public interface LocalCommand {

    void execute(InternalTaskCommandService internalTaskCommandService);

    boolean hasTask(TaskId taskId);
}
