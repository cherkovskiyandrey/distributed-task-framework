package com.distributed_task_framework.local_commands;

import com.distributed_task_framework.persistence.entity.TaskEntity;


public interface TaskBasedLocalCommand extends LocalCommand {
    TaskEntity taskEntity();
}
