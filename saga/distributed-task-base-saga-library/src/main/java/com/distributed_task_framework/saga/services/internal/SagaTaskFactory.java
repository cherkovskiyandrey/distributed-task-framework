package com.distributed_task_framework.saga.services.internal;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.task.SagaRevertTask;
import com.distributed_task_framework.saga.task.SagaTask;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;

import java.lang.reflect.Method;

public interface SagaTaskFactory {

    SagaTask sagaTask(TaskDef<SagaPipeline> taskDef,
                      Method method,
                      Object bean,
                      SagaMethodSettings sagaMethodSettings);

    SagaRevertTask sagaRevertTask(TaskDef<SagaPipeline> taskDef,
                                  Method method,
                                  Object bean);
}
