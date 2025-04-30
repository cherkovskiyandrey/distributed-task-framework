package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.model.TaskDef;
import lombok.Builder;
import lombok.Value;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Note: compare targetObject and proxyWrappers via reference only.
 */
@Value
@Builder
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SagaOperand that = (SagaOperand) o;
        return Objects.equals(method, that.method)
            && (targetObject == that.targetObject)
            && equalsCollection(proxyWrappers, that.proxyWrappers)
            && Objects.equals(taskDef, that.taskDef);
    }

    private boolean equalsCollection(Collection<Object> left, Collection<Object> right) {
        if (left.size() != right.size()) {
            return false;
        }

        var li = left.iterator();
        var ri = right.iterator();
        while (li.hasNext()) {
            if (li.next() != ri.next()) {
                return false;
            }
        }
        return true;
    }
}
