package com.distributed_task_framework.autoconfigure.annotation;

import com.distributed_task_framework.settings.TaskSettings;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenient approach to set execution guarantees.
 * Has precedence under default settings and overridden by custom settings in the config
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskExecutionGuarantees {

    /**
     * Execution guarantees.
     *
     * @return
     */
    TaskSettings.ExecutionGuarantees value() default TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE;
}
