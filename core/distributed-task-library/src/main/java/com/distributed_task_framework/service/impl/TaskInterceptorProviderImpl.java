package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.exception.TaskConfigurationException;
import com.distributed_task_framework.interceptor.CommonTaskCreationInterceptor;
import com.distributed_task_framework.interceptor.CommonTaskExecutionInterceptor;
import com.distributed_task_framework.interceptor.TaskCreationInterceptor;
import com.distributed_task_framework.interceptor.TaskExecutionInterceptor;
import com.distributed_task_framework.service.TaskInterceptorProvider;
import com.distributed_task_framework.settings.TaskSettings;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.lang.NonNull;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Resolves interceptors by classes from {@link TaskSettings}.
 * A configured class matches the first registered interceptor it is assignable from.
 * A configured class without a registered bean fails fast.
 */
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class TaskInterceptorProviderImpl implements TaskInterceptorProvider {
    List<TaskCreationInterceptor> creationInterceptors;
    List<TaskExecutionInterceptor> executionInterceptors;

    public TaskInterceptorProviderImpl(@NonNull List<TaskCreationInterceptor> creationInterceptors,
                                       @NonNull List<TaskExecutionInterceptor> executionInterceptors) {
        this.creationInterceptors = creationInterceptors;
        this.executionInterceptors = executionInterceptors;
    }

    @Override
    public @NonNull List<TaskCreationInterceptor> getCreationInterceptors(@NonNull TaskSettings taskSettings) {
        return combine(
            getCommonCreationInterceptors(),
            resolve(taskSettings.getCreationInterceptors(), creationInterceptors),
            taskSettings.getExcludedCommonCreationInterceptors()
        );
    }

    @Override
    public @NonNull List<TaskExecutionInterceptor> getExecutionInterceptors(@NonNull TaskSettings taskSettings) {
        return combine(
            getCommonExecutionInterceptors(),
            resolve(taskSettings.getExecutionInterceptors(), executionInterceptors),
            taskSettings.getExcludedCommonExecutionInterceptors()
        );
    }

    @Override
    public @NonNull List<TaskCreationInterceptor> getCommonCreationInterceptors() {
        return sort(creationInterceptors.stream()
            .filter(CommonTaskCreationInterceptor.class::isInstance)
            .toList()
        );
    }

    @Override
    public @NonNull List<TaskExecutionInterceptor> getCommonExecutionInterceptors() {
        return sort(executionInterceptors.stream()
            .filter(CommonTaskExecutionInterceptor.class::isInstance)
            .toList()
        );
    }

    private static <T> List<T> resolve(List<? extends Class<? extends T>> interceptorClasses,
                                       List<T> interceptors) {
        List<T> result = new ArrayList<>();
        for (Class<? extends T> interceptorClass : interceptorClasses) {
            T interceptor = interceptors.stream()
                // proxy safe: resolves by the user class in case of CGLIB proxies
                .filter(candidate -> interceptorClass.isAssignableFrom(ClassUtils.getUserClass(candidate)))
                .findFirst()
                .orElseThrow(() -> new TaskConfigurationException(
                    "Interceptor=[%s] is not registered as a bean, available interceptors=%s".formatted(
                        interceptorClass.getName(),
                        interceptors.stream()
                            .map(registered -> ClassUtils.getUserClass(registered).getName())
                            .toList()
                    )
                ));
            result.add(interceptor);
        }
        return result;
    }

    private static <T> List<T> combine(List<T> common, List<T> configured,
                                       List<? extends Class<? extends T>> excludedCommon) {
        List<T> result = new ArrayList<>(common);
        for (T interceptor : configured) {
            if (!result.contains(interceptor)) {
                result.add(interceptor);
            }
        }
        result.removeIf(interceptor -> isExcluded(interceptor, excludedCommon));
        return sort(result);
    }

    private static <T> boolean isExcluded(T interceptor, List<? extends Class<? extends T>> excludedCommon) {
        Class<?> userClass = ClassUtils.getUserClass(interceptor);
        return excludedCommon.stream().anyMatch(excluded -> excluded.isAssignableFrom(userClass));
    }

    private static <T> List<T> sort(List<T> interceptors) {
        List<T> sorted = new ArrayList<>(interceptors);
        // stable: respects @Order/Ordered, keeps common interceptors before configured otherwise
        sorted.sort(AnnotationAwareOrderComparator.INSTANCE);
        return sorted;
    }
}
