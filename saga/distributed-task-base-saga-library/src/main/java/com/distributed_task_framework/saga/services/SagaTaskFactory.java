package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.services.impl.SagaRevertTask;
import com.distributed_task_framework.saga.services.impl.SagaTask;

import java.lang.reflect.Method;

public interface SagaTaskFactory {

    SagaTask sagaTask(TaskDef<SagaEmbeddedPipelineContext> taskDef,
                      Method method,
                      Object bean,
                      SagaMethod sagaMethodAnnotation);

    SagaRevertTask sagaRevertTask(TaskDef<SagaEmbeddedPipelineContext> taskDef,
                                  Method method,
                                  Object bean);
}
