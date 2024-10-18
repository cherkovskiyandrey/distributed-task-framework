package com.distributed_task_framework.autoconfigure.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenient approach to set backoff retry policy.
 * Has precedence under default settings and overridden by custom settings in the config
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskBackoffRetryPolicy {
    /**
     * Initial delay of the first retry.
     * Must be compatible with {@link java.time.Duration#parse(CharSequence)}
     */
    String initialDelay() default "";

    /**
     * The time interval that is the ratio of the exponential backoff formula (geometric progression)
     * Must be compatible with {@link java.time.Duration#parse(CharSequence)}
     */
    String delayPeriod() default "";

    /**
     * Maximum number of times a tuple is retried before being acked and scheduled for commit.
     */
    int maxRetries() default -1;

    /**
     * Maximum amount of time waiting before retrying.
     * Must be compatible with {@link java.time.Duration#parse(CharSequence)}
     */
    String maxDelay() default "";
}
