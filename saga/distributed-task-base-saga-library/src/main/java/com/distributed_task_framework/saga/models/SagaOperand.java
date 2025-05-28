package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;


@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Value
@Builder(toBuilder = true)
public class SagaOperand {
    Method method;
    Object targetObject;
    @Builder.Default
    Collection<Object> proxyWrappers = List.of();
    TaskDef<SagaPipeline> taskDef;

    @Override
    public String toString() {
        return "SagaOperand{" +
            "method=" + method +
            ", targetObject=" + System.identityHashCode(targetObject) +
            ", proxyWrappers=" + proxyWrappers.stream().map(System::identityHashCode).toList() +
            ", taskDef=" + taskDef +
            '}';
    }
}
