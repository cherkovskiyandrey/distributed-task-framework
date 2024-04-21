package com.distributed_task_framework.test_service.services.impl;


import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.services.SagaRegister;
import com.distributed_task_framework.test_service.services.SagaTaskFactory;
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
    TaskSerializer taskSerializer;
    SagaHelper sagaHelper;

    @Override
    public SagaTask sagaTask(TaskDef<SagaPipelineContext> taskDef,
                             Method method,
                             Object bean,
                             SagaMethod sagaMethodAnnotation) {
        return new SagaTask(
                sagaRegister,
                distributedTaskService,
                taskSerializer,
                sagaHelper,
                taskDef,
                method,
                bean,
                sagaMethodAnnotation
        );
    }

    @Override
    public SagaRevertTask sagaRevertTask(TaskDef<SagaPipelineContext> taskDef, Method method, Object bean) {
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
