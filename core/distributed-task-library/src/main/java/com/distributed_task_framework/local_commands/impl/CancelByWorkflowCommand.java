package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.WorkflowBasedLocalCommand;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import lombok.Value;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Value(staticConstructor = "of")
public class CancelByWorkflowCommand implements WorkflowBasedLocalCommand {
    Collection<UUID> workflows;
    List<TaskId> excludes;

    @Override
    public Collection<UUID> workflows() {
        return workflows;
    }

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        internalTaskCommandService.cancelAll(workflows, excludes);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        //because this task isn't created for current task
        return false;
    }
}
