package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaFlowBuilder;
import com.distributed_task_framework.saga.services.SagaFlowBuilderWithoutInput;
import com.distributed_task_framework.saga.services.SagaFlowEntryPoint;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.service.DistributedTaskService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

//todo: like DisabledDistributedTaskAutoconfigure
@Slf4j
@AutoConfiguration
@ConditionalOnClass(DistributionSagaService.class)
@ConditionalOnExpression("#{environment['distributed-task.enabled'] != 'true' || environment['distributed-task.saga.enabled'] != 'true'}")
public class DisabledSagaAutoconfigure {

    @Bean
    @ConditionalOnMissingBean
    public DistributionSagaService disabledDistributionSagaService() {
        return (DistributionSagaService) Proxy.newProxyInstance(
            this.getClass().getClassLoader(),
            new Class[]{
                DistributionSagaService.class,
                SagaFlowEntryPoint.class,
                SagaFlow.class,
                SagaFlowBuilder.class,
                SagaFlowBuilderWithoutInput.class,
                SagaFlowWithoutResult.class
            },
            (proxy, method, args) -> {
                log.warn("Distributed saga service is disabled. To enable it set property 'distributed-task.enabled=true' " +
                        "and 'distributed-task.saga.enabled=true'. Called {}.{}()",
                    DistributedTaskService.class.getName(),
                    method.getName()
                );

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

                if (returnType.isAssignableFrom(UUID.class)) {
                    return UUID.randomUUID();
                }

                // inner classes
                if (returnType.isAssignableFrom(SagaFlowEntryPoint.class) ||
                    returnType.isAssignableFrom(SagaFlow.class) ||
                    returnType.isAssignableFrom(SagaFlowBuilder.class) ||
                    returnType.isAssignableFrom(SagaFlowBuilderWithoutInput.class) ||
                    returnType.isAssignableFrom(SagaFlowWithoutResult.class)) {
                    return proxy;
                }

                // Other.
                return null;
            }
        );
    }
}
