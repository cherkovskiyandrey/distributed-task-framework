package com.distributed_task_framework.local_commands.impl;

import com.distributed_task_framework.local_commands.TaskBasedLocalCommand;
import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.InternalTaskCommandService;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;

@ToString
public abstract class AbstractTaskBasedContextAwareCommand implements TaskBasedLocalCommand {
    @Getter
    @Nullable
    private AbstractTaskBasedContextAwareCommand nextCommand;
    protected TaskEntity taskEntity;

    protected AbstractTaskBasedContextAwareCommand(TaskEntity taskEntity) {
        this.taskEntity = taskEntity;
    }

    @Override
    public void execute(InternalTaskCommandService internalTaskCommandService) {
        var result = doExecute(internalTaskCommandService, taskEntity);
        if (nextCommand != null) {
            nextCommand.taskEntity = patchVersion(result, nextCommand.taskEntity);
            nextCommand.execute(internalTaskCommandService);
        }
    }

    private TaskEntity patchVersion(TaskEntity currentTaskEntity, TaskEntity nextTaskEntity) {
        return nextTaskEntity.toBuilder()
            .version(currentTaskEntity.getVersion())
            .build();
    }

    protected abstract TaskEntity doExecute(InternalTaskCommandService internalTaskCommandService,
                                            TaskEntity basedTaskEntity);

    @SuppressWarnings("DataFlowIssue")
    public AbstractTaskBasedContextAwareCommand doAfter(AbstractTaskBasedContextAwareCommand nextCommand) {
        var cmd = this;
        while (cmd.nextCommand != null) {
            cmd = cmd.getNextCommand();
        }
        cmd.nextCommand = nextCommand;
        return this;
    }

    @Override
    public TaskEntity taskEntity() {
        return taskEntity;
    }

    @Override
    public boolean hasTask(TaskId taskId) {
        return taskEntity.getId().equals(taskId.getId());
    }
}
