package com.distributed_task_framework.saga.services.impl;


import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.task.SagaRevertTask;
import com.distributed_task_framework.saga.task.SagaTask;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaTaskFactoryImpl implements SagaTaskFactory {
    SagaResolver sagaResolver;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;

    @Override
    public SagaTask sagaTask(TaskDef<SagaPipeline> taskDef,
                             Method method,
                             Object bean,
                             SagaMethodSettings sagaMethodSettings) {
        return new SagaTask(
            sagaResolver,
            sagaManager,
            distributedTaskService,
            taskSerializer,
            sagaHelper,
            taskDef,
            method,
            bean,
            sagaMethodSettings
        );
    }

    @Override
    public SagaRevertTask sagaRevertTask(TaskDef<SagaPipeline> taskDef, Method method, Object bean) {
        return new SagaRevertTask(
            sagaResolver,
            sagaManager,
            distributedTaskService,
            sagaHelper,
            taskDef,
            method,
            bean
        );
    }
}
