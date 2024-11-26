package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.TaskDefBasedLocalCommand;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import lombok.Value;

import java.util.List;

@Value(staticConstructor = "of")
public class CancelByTaskDefCommand implements TaskDefBasedLocalCommand {
    TaskDef<?> taskDef;
    List<TaskId> excludes;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.cancelAll(taskDef, excludes);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        //because this task isn't created for current task
        return false;
    }

    @Override
    public TaskDef<?> taskDef() {
        return taskDef;
    }
}
