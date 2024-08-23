package com.distributed_task_framework.task;

import com.distributed_task_framework.model.RegisteredTask;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.settings.TaskSettings;

import java.util.function.BiConsumer;

public class TaskWorkerGenerator {

    public static <T> TaskWorker defineTaskWorker(BiConsumer<TaskEntity, RegisteredTask<T>> executor) {
        return new TaskWorker() {
            @Override
            public boolean isApplicable(TaskEntity taskEntity, TaskSettings taskParameters) {
                return true;
            }

            @Override
            public <U> void execute(TaskEntity taskEntity, RegisteredTask<U> registeredTask) {
                executor.accept(taskEntity, (RegisteredTask<T>) registeredTask);
            }
        };
    }

}
