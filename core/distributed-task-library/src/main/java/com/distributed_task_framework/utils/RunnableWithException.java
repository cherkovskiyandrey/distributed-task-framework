package com.distributed_task_framework.utils;

@FunctionalInterface
public interface RunnableWithException {
    void execute() throws Exception;
}
