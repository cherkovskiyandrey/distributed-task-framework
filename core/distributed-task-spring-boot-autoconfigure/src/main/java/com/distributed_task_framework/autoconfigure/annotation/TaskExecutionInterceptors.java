package com.distributed_task_framework.autoconfigure.annotation;

import com.distributed_task_framework.interceptor.CommonTaskExecutionInterceptor;
import com.distributed_task_framework.interceptor.TaskExecutionInterceptor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenient approach to attach execution interceptors to a task.
 * Combined with default interceptors from the config and deduplicated.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskExecutionInterceptors {

    /**
     * Classes of {@link TaskExecutionInterceptor} implementations.
     *
     * @return
     */
    Class<? extends TaskExecutionInterceptor>[] value() default {};

    /**
     * Classes of common execution interceptors excluded for the task.
     *
     * @return
     */
    Class<? extends CommonTaskExecutionInterceptor>[] excludedCommon() default {};
}
