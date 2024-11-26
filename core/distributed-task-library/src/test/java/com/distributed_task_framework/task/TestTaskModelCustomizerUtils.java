package com.distributed_task_framework.task;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.function.Function;

@UtilityClass
public class TestTaskModelCustomizerUtils {

    public static <T> TaskGenerator.Consumer<ExecutionContext<T>> throwException() {
        return ctx -> {
            throw new RuntimeException();
        };
    }

    public static Function<TaskEntity, TaskEntity> inVirtualQueue(VirtualQueue virtualQueue) {
        return taskEntity -> taskEntity.toBuilder()
            .virtualQueue(virtualQueue)
            .deletedAt(virtualQueue == VirtualQueue.DELETED ? LocalDateTime.now() : null)
            .build();
    }

    public static Function<TaskEntity, TaskEntity> removed() {
        return taskEntity -> taskEntity.toBuilder()
            .virtualQueue(VirtualQueue.DELETED)
            .deletedAt(LocalDateTime.now())
            .build();
    }

    public static Function<TaskEntity, TaskEntity> canceled() {
        return taskEntity -> taskEntity.toBuilder()
            .canceled(true)
            .build();
    }

    public static <T> TestTaskModelSpec<T> assigned(Class<T> cls) {
        return TestTaskModelSpec.builder(cls)
            .withSaveInstance()
            .taskEntityCustomizer(taskEntity -> taskEntity.toBuilder()
                .assignedWorker(UUID.randomUUID())
                .build()
            )
            .build();
    }
}
