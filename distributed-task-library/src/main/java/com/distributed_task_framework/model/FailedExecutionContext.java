package com.distributed_task_framework.model;


import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@ToString(callSuper = true)
@Getter
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@SuperBuilder(toBuilder = true)
@Jacksonized
public class FailedExecutionContext<T> extends ExecutionContext<T> {
    /**
     * Last occurred error.
     */
    Throwable error;
    /**
     * Number of already failures.
     */
    int failures;
    /**
     * Is it last attempt to retry
     */
    boolean isLastAttempt;
}
