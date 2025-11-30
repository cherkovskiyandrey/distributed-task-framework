package com.distributed_task_framework.utils;

@FunctionalInterface
public interface SupplierWithException<U> {
    U get() throws Exception;
}
