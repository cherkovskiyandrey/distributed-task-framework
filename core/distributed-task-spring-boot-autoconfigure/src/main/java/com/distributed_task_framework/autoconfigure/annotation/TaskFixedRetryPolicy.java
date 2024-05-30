package com.distributed_task_framework.autoconfigure.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenient approach to set fixed retry policy.
 * Has precedence under default settings and overridden by custom settings in the config
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskFixedRetryPolicy {

    /**
     * Delay between retires.
     * Must be compatible with {@link java.time.Duration#parse(CharSequence)}
     */
    String delay() default "";

    /**
     * Max attempts
     */
    int number() default -1;

    /**
     * Max interval for retires.
     * Give up after whether max attempts is reached or interval is passed.
     * Must be compatible with {@link java.time.Duration#parse(CharSequence)}
     */
    String maxInterval() default "";
}
