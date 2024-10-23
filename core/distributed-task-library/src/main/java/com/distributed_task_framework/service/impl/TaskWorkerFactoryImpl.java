package com.distributed_task_framework.service.impl;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.service.internal.TaskWorker;
import com.distributed_task_framework.service.internal.TaskWorkerFactory;
import com.distributed_task_framework.settings.TaskSettings;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskWorkerFactoryImpl implements TaskWorkerFactory {
    List<TaskWorker> taskWorkerList;

    @Override
    public TaskWorker buildTaskWorker(TaskEntity taskEntity, TaskSettings taskSettings) {
        return taskWorkerList.stream()
                .filter(taskWorker -> taskWorker.isApplicable(taskEntity, taskSettings))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Can't find handler for task=[%s]".formatted(taskEntity)));
    }
}
