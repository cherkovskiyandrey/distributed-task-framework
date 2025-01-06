package com.distributed_task_framework.saga.models;

import lombok.Value;

import java.util.List;

@Value
public class SagaMethod {
    String underlingClass;
    String methodName;
    List<String> parameterTypes;
    String returnType;
}
