package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.models.SagaMethod;
import lombok.experimental.UtilityClass;

import java.lang.reflect.Method;
import java.util.Arrays;

@UtilityClass
public class MethodSagaMethodFactory {

    public static SagaMethod of(Method method) {
        return new SagaMethod(
            method.getDeclaringClass().getTypeName(),
            method.getName(),
            Arrays.stream(method.getParameters())
                .map(parameter -> parameter.getType().getTypeName())
                .toList(),
            method.getReturnType().getTypeName()
        );
    }
}
