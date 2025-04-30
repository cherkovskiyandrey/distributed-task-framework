package com.distributed_task_framework.saga.models;

/**
 * Note: compare targetObject via reference only.
 *
 * @param targetObject
 * @param sagaMethod
 */
public record MethodReference(
    Object targetObject,
    SagaMethod sagaMethod
) {
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        MethodReference that = (MethodReference) other;
        return (sagaMethod == that.sagaMethod) && (targetObject == that.targetObject);
    }

    @Override
    public String toString() {
        return "MethodReference{" +
            "targetObject=" + System.identityHashCode(targetObject) +
            ", sagaMethod=" + sagaMethod +
            '}';
    }
}
