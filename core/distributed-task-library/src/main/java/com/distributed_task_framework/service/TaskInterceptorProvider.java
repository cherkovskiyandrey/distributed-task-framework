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
     * Common interceptors (minus excluded ones) first, then configured ones.
     *
     * @return interceptors in configured order
     */
    @NonNull
    List<TaskCreationInterceptor> getCreationInterceptors(@NonNull TaskSettings taskSettings);

    /**
     * Common interceptors (minus excluded ones) first, then configured ones.
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
