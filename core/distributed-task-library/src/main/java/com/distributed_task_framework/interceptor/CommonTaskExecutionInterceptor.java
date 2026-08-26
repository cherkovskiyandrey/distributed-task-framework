package com.distributed_task_framework.interceptor;

/**
 * Marker of an execution interceptor applied to all tasks by default:
 * it is invoked even if not attached to a task in the config, it is enough to declare a bean.
 * Can be excluded for a specific task via {@code excluded-common-execution-interceptors} config
 * or {@code @TaskExecutionInterceptors(excludedCommon = ...)} annotation.
 */
public interface CommonTaskExecutionInterceptor extends TaskExecutionInterceptor {
}
