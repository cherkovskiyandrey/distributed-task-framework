package com.distributed_task_framework.test_service.exceptions;

public class SagaExecutionException extends SagaException {
    public SagaExecutionException(String message,
                                  Throwable cause,
                                  boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
