package com.distributed_task_framework.exception;

import com.distributed_task_framework.model.TaskDef;

public class CronExpiredException extends RuntimeException {

    public <T> CronExpiredException(TaskDef<T> taskDef, String cron) {
        super("Task by taskDef=[%s] can't be scheduled because cron=[%s] is expired".formatted(taskDef, cron));
    }
}
