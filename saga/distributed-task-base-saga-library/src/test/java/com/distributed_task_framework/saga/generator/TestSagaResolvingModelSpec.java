package com.distributed_task_framework.saga.generator;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder(toBuilder = true)
public class TestSagaResolvingModelSpec {
    Object object;
    Class<?> lookupInClass;
    String methodName;
    @Builder.Default
    List<Class<?>> parameters = List.of();
    Class<?> returnType;
    @Builder.Default
    boolean withProxy = false;
    @Builder.Default
    boolean register = false;
}
