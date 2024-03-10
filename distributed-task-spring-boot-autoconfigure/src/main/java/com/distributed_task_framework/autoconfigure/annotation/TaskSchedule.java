package com.distributed_task_framework.autoconfigure.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenient approach to set task scheduling.
 * Has precedence under default settings and overridden by custom settings in the config
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskSchedule {

    /**
     * Cron string.
     *
     * @return cron string
     */
    String cron();
}
