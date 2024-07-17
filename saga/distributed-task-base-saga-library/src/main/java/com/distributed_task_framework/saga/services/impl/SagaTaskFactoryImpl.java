package com.distributed_task_framework.saga.services.impl;


import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.services.SagaContextService;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.saga.services.SagaTaskFactory;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaTaskFactoryImpl implements SagaTaskFactory {
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaContextService sagaContextService;
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;

    @Override
    public SagaTask sagaTask(TaskDef<SagaEmbeddedPipelineContext> taskDef,
                             Method method,
                             Object bean,
                             SagaMethod sagaMethodAnnotation) {
        return new SagaTask(
                sagaRegister,
                distributedTaskService,
                sagaContextService,
                taskSerializer,
                sagaHelper,
                taskDef,
                method,
                bean,
                sagaMethodAnnotation
        );
    }

    @Override
    public SagaRevertTask sagaRevertTask(TaskDef<SagaEmbeddedPipelineContext> taskDef, Method method, Object bean) {
        return new SagaRevertTask(
                sagaRegister,
                distributedTaskService,
                sagaHelper,
                taskDef,
                method,
                bean
        );
    }
}
