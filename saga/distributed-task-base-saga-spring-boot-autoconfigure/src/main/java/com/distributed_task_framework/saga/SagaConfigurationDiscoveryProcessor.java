package com.distributed_task_framework.saga;

import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.mappers.SagaMethodPropertiesMapper;
import com.distributed_task_framework.saga.mappers.SagaMethodPropertiesMerger;
import com.distributed_task_framework.saga.mappers.SagaPropertiesMapper;
import com.distributed_task_framework.saga.mappers.SagaPropertiesMerger;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.utils.ReflectionHelper;
import jakarta.annotation.PostConstruct;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
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

    @PostConstruct
    public void init() {
        registerConfiguredSagas();
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
            .filter(method -> ReflectionHelper.findAnnotation(method, SagaMethod.class).isPresent())
            .forEach(method -> {
                var taskName = SagaNamingUtils.taskNameFor(method);
                var sagaMethodSettings = buildSagaMethodSettings(method);

                distributionSagaService.registerSagaMethod(
                    taskName,
                    method,
                    bean,
                    sagaMethodSettings
                );
            });
    }

    private void registerSagaRevertMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
            .filter(method -> ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).isPresent())
            .forEach(method -> {
                var revertTaskName = SagaNamingUtils.taskNameFor(method);
                var sagaMethodSettings = buildSagaMethodSettings(method);

                distributionSagaService.registerSagaMethod(
                    revertTaskName,
                    method,
                    bean,
                    sagaMethodSettings
                );
            });
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
    private SagaMethodSettings buildSagaMethodSettings(Method method) {
        var sagaMethodPropertiesGroup = Optional.ofNullable(distributedSagaProperties.getSagaMethodPropertiesGroup());

        var sagaMethodDefaultCodeProperties = sagaMethodPropertiesMapper.map(SagaMethodSettings.DEFAULT);
        var sagaMethodCustomCodeProperties = fillCustomProperties(method);

        var sagaMethodDefaultConfProperties = sagaMethodPropertiesGroup
            .map(DistributedSagaProperties.SagaMethodPropertiesGroup::getDefaultSagaMethodProperties)
            .orElse(null);
        var sagaMethodCustomConfProperties = sagaMethodPropertiesGroup
            .map(DistributedSagaProperties.SagaMethodPropertiesGroup::getSagaMethodProperties)
            .map(sagaMethodProperties -> sagaMethodProperties.get(SagaNamingUtils.sagaMethodNameFor(method)))
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
        ReflectionHelper.findAnnotation(method, Transactional.class)
            .ifPresent(executionGuarantees ->
                sagaMethodProperties.setExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
            );
    }
}
