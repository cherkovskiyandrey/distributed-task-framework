package com.distributed_task_framework.exception;

import lombok.Getter;
import org.springframework.dao.OptimisticLockingFailureException;

public class OptimisticLockException extends OptimisticLockingFailureException {
    @Getter
    private final Class<?> entityClass;

    public OptimisticLockException(String msg, Class<?> entityClass) {
        super(msg);
        this.entityClass = entityClass;
    }
}
