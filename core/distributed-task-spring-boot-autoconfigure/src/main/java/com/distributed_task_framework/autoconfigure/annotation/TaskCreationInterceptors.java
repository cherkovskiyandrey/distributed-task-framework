package com.distributed_task_framework.autoconfigure.annotation;

import com.distributed_task_framework.interceptor.CommonTaskCreationInterceptor;
import com.distributed_task_framework.interceptor.TaskCreationInterceptor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Convenient approach to attach creation interceptors to a task.
 * Combined with default interceptors from the config and deduplicated.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TaskCreationInterceptors {

    /**
     * Classes of {@link TaskCreationInterceptor} implementations.
     *
     * @return
     */
    Class<? extends TaskCreationInterceptor>[] value() default {};

    /**
     * Classes of common creation interceptors excluded for the task.
     *
     * @return
     */
    Class<? extends CommonTaskCreationInterceptor>[] excludedCommon() default {};
}
