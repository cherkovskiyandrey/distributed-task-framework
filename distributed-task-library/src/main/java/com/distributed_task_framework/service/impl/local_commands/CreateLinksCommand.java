package com.distributed_task_framework.service.impl.local_commands;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.internal.TaskLinkManager;
import lombok.Builder;
import lombok.Value;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import com.distributed_task_framework.service.internal.LocalCommand;

import java.util.List;

@Value
@Builder
public class CreateLinksCommand implements LocalCommand {
    TaskId joinTaskId;
    List<TaskId> joinList;
    TaskLinkManager taskLinkManager;

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        taskLinkManager.createLinks(joinTaskId, joinList);
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return false;
    }
}
