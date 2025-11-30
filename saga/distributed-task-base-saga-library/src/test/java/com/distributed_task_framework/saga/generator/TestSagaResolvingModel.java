package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.models.SagaOperand;
import com.distributed_task_framework.saga.models.SagaPipeline;
import lombok.Builder;
import lombok.Value;

import java.lang.reflect.Method;

@Value
@Builder
public class TestSagaResolvingModel<T> {
    T targetObject;
    T proxy;
    TaskDef<SagaPipeline> taskDef;
    String operandName;
    SagaOperand sagaOperand;
    Method method;
}
