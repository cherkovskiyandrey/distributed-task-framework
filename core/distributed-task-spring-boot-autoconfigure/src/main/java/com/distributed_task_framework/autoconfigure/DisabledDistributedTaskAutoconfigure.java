package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.service.DistributedTaskService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Page;

import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@AutoConfiguration
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "false", matchIfMissing = true)
@Slf4j
public class DisabledDistributedTaskAutoconfigure {
    public static final TaskId DUMMY_TASK_ID = new TaskId(
        "stub",
        "stub",
        new UUID(0, 0),
        new UUID(0, 0)
    );
    public static final Page<?> EMPTY_PAGE = Page.empty();

    // Build java proxy stub.
    @Bean
    @ConditionalOnMissingBean
    public DistributedTaskService disabledDistributedTaskService() {
        return (DistributedTaskService) Proxy.newProxyInstance(
            this.getClass().getClassLoader(),
            new Class[]{DistributedTaskService.class},
            (proxy, method, args) -> {
                log.warn("Distributed task framework is disabled. To enable it set property 'distributed-task.enabled=true'. " +
                    "Called {}.{}()", DistributedTaskService.class.getName(), method.getName());

                final Class<?> returnType = method.getReturnType();
                if (returnType.isPrimitive()) {
                    if (returnType == boolean.class) {
                        return false;
                    }

                    if (returnType == float.class || returnType == double.class) {
                        return 0.0F;
                    }

                    // numeric primitives
                    return 0;
                }

                // Collections.
                if (returnType.isAssignableFrom(Set.class)) {
                    return Set.of();
                }

                if (returnType.isAssignableFrom(Map.class)) {
                    return Map.of();
                }

                if (returnType.isAssignableFrom(List.class) || returnType.isAssignableFrom(Collection.class)) {
                    return List.of();
                }

                if (returnType == Optional.class) {
                    return Optional.empty();
                }

                if (returnType == TaskId.class) {
                    return DUMMY_TASK_ID;
                }

                if (returnType.isAssignableFrom(Page.class)) {
                    return EMPTY_PAGE;
                }

                // Other.
                return null;
            }
        );
    }
}
