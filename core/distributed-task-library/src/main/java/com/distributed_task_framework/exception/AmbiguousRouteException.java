package com.distributed_task_framework.exception;

public class AmbiguousRouteException extends RuntimeException {
    public AmbiguousRouteException(String message) {
        super(message);
    }
}
