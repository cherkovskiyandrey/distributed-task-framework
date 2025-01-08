package com.distributed_task_framework.exception;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.model.TaskId;

import java.util.UUID;

public class UnknownTaskException extends RuntimeException {

    public <T> UnknownTaskException(TaskDef<T> taskDef) {
        super("Task by name=[%s] has not been registered or already unregistered".formatted(taskDef));
    }

    public UnknownTaskException(TaskId taskId) {
        super("Task by taskId=[%s] has not been registered or already unregistered".formatted(taskId));
    }

    public UnknownTaskException(UUID taskId) {
        super("Task by id=[%s] doesn't exists".formatted(taskId));
    }
}
