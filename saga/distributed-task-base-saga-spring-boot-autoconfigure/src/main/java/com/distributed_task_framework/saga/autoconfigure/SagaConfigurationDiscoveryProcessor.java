package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaSpecific;
import com.distributed_task_framework.saga.autoconfigure.exceptions.SagaBeanInitException;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaMethodPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaMethodPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.utils.ReflectionHelper;
import com.distributed_task_framework.saga.autoconfigure.utils.SagaNamingUtils;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.Maps;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaConfigurationDiscoveryProcessor implements BeanPostProcessor {
    DistributionSagaService distributionSagaService;
    DistributedSagaProperties distributedSagaProperties;
    SagaMethodPropertiesMapper sagaMethodPropertiesMapper;
    SagaMethodPropertiesMerger sagaMethodPropertiesMerger;
    SagaPropertiesMapper sagaPropertiesMapper;
    SagaPropertiesMerger sagaPropertiesMerger;
    Map<Object, com.distributed_task_framework.saga.autoconfigure.utils.ReflectionHelper.ProxyObject> beansToProxyObject = Maps.newIdentityHashMap();

    @PostConstruct
    public void init() {
        registerConfiguredSagas();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (!isIgnoredAsSagaBean(bean)) {
            registerSagaMethodIfExists(bean);
            registerSagaRevertMethodIfExists(bean);
        }
        return bean;
    }

    private boolean isIgnoredAsSagaBean(Object bean) {
        return bean instanceof SagaSpecific sagaSpecific && sagaSpecific.ignore();
    }

    //todo: integration tests with SagaSpecific and several beans
    private void registerSagaMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
            .filter(method -> com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, SagaMethod.class).isPresent())
            .forEach(method -> {
                var suffix = buildSuffix(bean);
                var taskName = SagaNamingUtils.taskNameFor(method, suffix);
                var sagaMethodSettings = buildSagaMethodSettings(method, suffix);
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
            .filter(method -> com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).isPresent())
            .forEach(method -> {
                var suffix = buildSuffix(bean);
                var revertTaskName = SagaNamingUtils.taskNameFor(method, suffix);
                var sagaMethodSettings = buildSagaMethodSettings(method, suffix);
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

    private String buildSuffix(Object bean) {
        String suffix = "";
        if (bean instanceof SagaSpecific sagaSpecific) {
            if (sagaSpecific.ignore()) {
                log.info("buildSuffix(): bean={} is marked as saga ignored", bean);
            } else {
                suffix = sagaSpecific.suffix();
                if (StringUtils.isBlank(suffix)) {
                    throw new SagaBeanInitException("empty saga prefix for bean=[%s]".formatted(bean));
                }
            }
        }
        return suffix;
    }

    //Right ordering:
    //1. sagaDefaultCodeProperties
    //2. sagaDefaultConfProperties
    //3. sagaCustomConfProperties
    private void registerConfiguredSagas() {
        var sagaPropertiesGroup = Optional.ofNullable(distributedSagaProperties.getSagaPropertiesGroup());

        var sagaDefaultCodeProperties = sagaPropertiesMapper.map(SagaSettings.DEFAULT);
        var defaultSagaConfProperties = sagaPropertiesGroup
            .map(DistributedSagaProperties.SagaPropertiesGroup::getDefaultSagaProperties)
            .orElse(null);

        var sagaDefaultProperties = sagaPropertiesMerger.merge(sagaDefaultCodeProperties, defaultSagaConfProperties);
        var sagaDefaultSettings = sagaPropertiesMapper.map(sagaDefaultProperties);

        distributionSagaService.registerDefaultSagaSettings(sagaDefaultSettings);

        sagaPropertiesGroup
            .map(DistributedSagaProperties.SagaPropertiesGroup::getSagaPropertiesGroup)
            .map(Map::entrySet)
            .stream()
            .flatMap(Collection::stream)
            .forEach(entry -> {
                var sagaCustomProperties = sagaPropertiesMerger.merge(sagaDefaultProperties, entry.getValue());
                var sagaCustomSettings = sagaPropertiesMapper.map(sagaCustomProperties);

                distributionSagaService.registerSagaSettings(
                    entry.getKey(),
                    sagaCustomSettings
                );
            });
    }

    //Right ordering:
    //1. sagaMethodDefaultCodeProperties
    //2. sagaMethodDefaultConfProperties
    //3. sagaMethodCustomCodeProperties
    //4. sagaMethodCustomConfProperties
    private SagaMethodSettings buildSagaMethodSettings(Method method, @Nullable String suffix) {
        var sagaMethodPropertiesGroup = Optional.ofNullable(distributedSagaProperties.getSagaMethodPropertiesGroup());

        var sagaMethodDefaultCodeProperties = sagaMethodPropertiesMapper.map(SagaMethodSettings.DEFAULT);
        var sagaMethodCustomCodeProperties = fillCustomProperties(method);

        var sagaMethodDefaultConfProperties = sagaMethodPropertiesGroup
            .map(DistributedSagaProperties.SagaMethodPropertiesGroup::getDefaultSagaMethodProperties)
            .orElse(null);
        var sagaMethodCustomConfProperties = sagaMethodPropertiesGroup
            .map(DistributedSagaProperties.SagaMethodPropertiesGroup::getSagaMethodProperties)
            .map(sagaMethodProperties -> sagaMethodProperties.get(SagaNamingUtils.sagaMethodNameFor(method, suffix)))
            .orElse(null);

        var sagaMethodDefaultProperties = sagaMethodPropertiesMerger.merge(
            sagaMethodDefaultCodeProperties,
            sagaMethodDefaultConfProperties
        );
        var sagaMethodCustomProperties = sagaMethodPropertiesMerger.merge(
            sagaMethodCustomCodeProperties,
            sagaMethodCustomConfProperties
        );

        var sagaMethodProperties = sagaMethodPropertiesMerger.merge(
            sagaMethodDefaultProperties,
            sagaMethodCustomProperties
        );

        return sagaMethodPropertiesMapper.map(sagaMethodProperties);
    }

    private DistributedSagaProperties.SagaMethodProperties fillCustomProperties(Method method) {
        var taskProperties = new DistributedSagaProperties.SagaMethodProperties();
        fillExecutionGuarantees(method, taskProperties);
        return taskProperties;
    }

    private void fillExecutionGuarantees(Method method, DistributedSagaProperties.SagaMethodProperties sagaMethodProperties) {
        com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, Transactional.class)
            .ifPresent(executionGuarantees ->
                sagaMethodProperties.setExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
            );
    }
}
