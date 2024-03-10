package com.distributed_task_framework.exception;

public class PostponedTaskException extends RuntimeException {
    public PostponedTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
