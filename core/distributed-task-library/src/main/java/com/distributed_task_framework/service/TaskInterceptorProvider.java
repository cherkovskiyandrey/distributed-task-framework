package com.distributed_task_framework.service;

import com.distributed_task_framework.interceptor.TaskCreationInterceptor;
import com.distributed_task_framework.interceptor.TaskExecutionInterceptor;
import com.distributed_task_framework.settings.TaskSettings;
import org.springframework.lang.NonNull;

import java.util.List;

/**
 * Resolves interceptors attached to a task by its settings.
 */
public interface TaskInterceptorProvider {
    /**
     * Common interceptors (minus excluded ones), then configured ones;
     * the combined list is sorted by {@code @Order} / {@code Ordered}, so a configured interceptor
     * with a higher priority runs before common ones without explicit order.
     *
     * @return interceptors in configured order
     */
    @NonNull
    List<TaskCreationInterceptor> getCreationInterceptors(@NonNull TaskSettings taskSettings);

    /**
     * Common interceptors (minus excluded ones), then configured ones;
     * the combined list is sorted by {@code @Order} / {@code Ordered}, so a configured interceptor
     * with a higher priority runs before common ones without explicit order.
     *
     * @return interceptors in configured order
     */
    @NonNull
    List<TaskExecutionInterceptor> getExecutionInterceptors(@NonNull TaskSettings taskSettings);

    /**
     * @return common creation interceptors in configured order
     */
    @NonNull
    List<TaskCreationInterceptor> getCommonCreationInterceptors();

    /**
     * @return common execution interceptors in configured order
     */
    @NonNull
    List<TaskExecutionInterceptor> getCommonExecutionInterceptors();
}
