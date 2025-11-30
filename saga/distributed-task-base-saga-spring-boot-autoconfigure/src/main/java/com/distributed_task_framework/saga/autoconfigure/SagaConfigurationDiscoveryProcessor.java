package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaSpecific;
import com.distributed_task_framework.saga.autoconfigure.exceptions.SagaBeanInitException;
import com.distributed_task_framework.saga.autoconfigure.services.SagaPropertiesProcessor;
import com.distributed_task_framework.saga.autoconfigure.utils.ReflectionHelper;
import com.distributed_task_framework.saga.autoconfigure.utils.SagaNamingUtils;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.google.common.collect.Maps;
import jakarta.annotation.PostConstruct;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaConfigurationDiscoveryProcessor implements BeanPostProcessor {
    private static final Set<String> IGNORE_METHOD_NAMES = Set.of(
        "equals",
        "hashCode",
        "toString",
        "wait",
        "notify",
        "getClass",
        "notifyAll",
        "finalize",
        "clone"
    );

    DistributionSagaService distributionSagaService;
    DistributedSagaProperties distributedSagaProperties;
    SagaPropertiesProcessor sagaPropertiesProcessor;
    Map<Object, com.distributed_task_framework.saga.autoconfigure.utils.ReflectionHelper.ProxyObject> beansToProxyObject = Maps.newIdentityHashMap();

    @PostConstruct
    public void init() {
        sagaPropertiesProcessor.registerConfiguredSagas(
            distributionSagaService,
            distributedSagaProperties.getSagaPropertiesGroup()
        );
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        registerSagaMethodIfExists(bean);
        registerSagaRevertMethodIfExists(bean);
        return bean;
    }

    private void registerSagaMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
            .filter(this::isNotIgnoredMethod)
            .filter(method -> com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, SagaMethod.class).isPresent())
            .forEach(method -> {
                var suffix = buildSuffix(bean);
                var taskName = SagaNamingUtils.taskNameFor(method, suffix);
                var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                    method,
                    suffix,
                    distributedSagaProperties.getSagaMethodPropertiesGroup()
                );
                var proxyObject = beansToProxyObject.computeIfAbsent(bean, k -> ReflectionHelper.unwrapSpringBean(bean));

                distributionSagaService.registerSagaMethod(
                    taskName,
                    method,
                    proxyObject.targetObject(),
                    proxyObject.proxyWrappers(),
                    sagaMethodSettings
                );
            });
    }

    private void registerSagaRevertMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
            .filter(this::isNotIgnoredMethod)
            .filter(method -> com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).isPresent())
            .forEach(method -> {
                var suffix = buildSuffix(bean);
                var revertTaskName = SagaNamingUtils.taskNameFor(method, suffix);
                var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                    method,
                    suffix,
                    distributedSagaProperties.getSagaMethodPropertiesGroup()
                );
                var proxyObject = beansToProxyObject.computeIfAbsent(bean, k -> ReflectionHelper.unwrapSpringBean(bean));

                distributionSagaService.registerSagaRevertMethod(
                    revertTaskName,
                    method,
                    proxyObject.targetObject(),
                    proxyObject.proxyWrappers(),
                    sagaMethodSettings
                );
            });
    }

    private boolean isNotIgnoredMethod(Method method) {
        return IGNORE_METHOD_NAMES.stream()
            .noneMatch(methodName -> method.getName().contains(methodName));
    }

    private String buildSuffix(Object bean) {
        String suffix = null;
        if (bean instanceof SagaSpecific sagaSpecific) {
            suffix = sagaSpecific.suffix();
            if (StringUtils.isBlank(suffix)) {
                throw new SagaBeanInitException("empty saga prefix for bean=[%s]".formatted(bean));
            }
        }
        return suffix;
    }
}
