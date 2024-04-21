package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.test_service.annotations.SagaMethod;
import com.distributed_task_framework.test_service.models.SagaPipelineContext;
import com.distributed_task_framework.test_service.services.impl.SagaRevertTask;
import com.distributed_task_framework.test_service.services.impl.SagaTask;

import java.lang.reflect.Method;

public interface SagaTaskFactory {

    SagaTask sagaTask(TaskDef<SagaPipelineContext> taskDef,
                      Method method,
                      Object bean,
                      SagaMethod sagaMethodAnnotation);

    SagaRevertTask sagaRevertTask(TaskDef<SagaPipelineContext> taskDef,
                                  Method method,
                                  Object bean);
}
