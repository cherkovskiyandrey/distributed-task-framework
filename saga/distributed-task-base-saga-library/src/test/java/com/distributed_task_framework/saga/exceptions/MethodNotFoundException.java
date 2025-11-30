package com.distributed_task_framework.saga.exceptions;

public class MethodNotFoundException extends RuntimeException {
    public MethodNotFoundException(String message) {
        super(message);
    }
}
