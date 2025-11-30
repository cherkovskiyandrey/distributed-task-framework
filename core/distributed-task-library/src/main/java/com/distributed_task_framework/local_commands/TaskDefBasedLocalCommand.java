package com.distributed_task_framework.local_commands;

import com.distributed_task_framework.model.TaskDef;

public interface TaskDefBasedLocalCommand extends LocalCommand {
    TaskDef<?> taskDef();
}
