package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.model.TaskDef;
import lombok.Value;

import java.lang.reflect.Method;

@Value
public class SagaOperand {
    Method method;
    TaskDef<SagaPipeline> taskDef;
}
