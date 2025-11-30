package com.distributed_task_framework.saga.autoconfigure.services.impl;

import com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaCommonPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaCommonPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaMethodPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaMethodPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaStatPropertiesMapper;
import com.distributed_task_framework.saga.autoconfigure.mappers.SagaStatPropertiesMerger;
import com.distributed_task_framework.saga.autoconfigure.services.SagaPropertiesProcessor;
import com.distributed_task_framework.saga.autoconfigure.utils.SagaNamingUtils;
import com.distributed_task_framework.saga.services.SagaRegisterSettingsService;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.saga.settings.SagaStatSettings;
import com.distributed_task_framework.settings.TaskSettings;
import com.google.common.collect.Lists;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class SagaPropertiesProcessorImpl implements SagaPropertiesProcessor {
    SagaCommonPropertiesMapper sagaCommonPropertiesMapper;
    SagaCommonPropertiesMerger sagaCommonPropertiesMerger;
    SagaStatPropertiesMapper sagaStatPropertiesMapper;
    SagaStatPropertiesMerger sagaStatPropertiesMerger;
    SagaPropertiesMapper sagaPropertiesMapper;
    SagaPropertiesMerger sagaPropertiesMerger;
    SagaMethodPropertiesMapper sagaMethodPropertiesMapper;
    SagaMethodPropertiesMerger sagaMethodPropertiesMerger;

    @Override
    public SagaCommonSettings buildSagaCommonSettings(@Nullable DistributedSagaProperties.Common common) {
        var sagaCommonConfProperties = sagaCommonPropertiesMapper.map(SagaCommonSettings.buildDefault());
        var sagsCommonProperties = sagaCommonPropertiesMerger.merge(
            sagaCommonConfProperties,
            common
        );
        return sagaCommonPropertiesMapper.map(sagsCommonProperties);
    }

    @Override
    public SagaStatSettings buildSagaStatSettings(@Nullable DistributedSagaProperties.SagaStatProperties sagaStatProperties) {
        var sagaStatConfProperties = sagaStatPropertiesMapper.map(SagaStatSettings.buildDefault());
        var sagsCommonProperties = sagaStatPropertiesMerger.merge(
            sagaStatConfProperties,
            sagaStatProperties
        );
        return sagaStatPropertiesMapper.map(sagsCommonProperties);
    }

    //Right ordering:
    //1. sagaDefaultCodeProperties
    //2. sagaDefaultConfProperties
    //3. sagaCustomConfProperties
    @Override
    public void registerConfiguredSagas(SagaRegisterSettingsService sagaRegisterSettingsService,
                                        @Nullable DistributedSagaProperties.SagaPropertiesGroup sagaPropertiesGroup) {
        var sagaPropertiesGroupOpt = Optional.ofNullable(sagaPropertiesGroup);

        var sagaDefaultCodeProperties = sagaPropertiesMapper.map(SagaSettings.DEFAULT);
        var defaultSagaConfProperties = sagaPropertiesGroupOpt
            .map(DistributedSagaProperties.SagaPropertiesGroup::getDefaultSagaProperties)
            .orElse(null);

        var sagaDefaultProperties = sagaPropertiesMerger.merge(sagaDefaultCodeProperties, defaultSagaConfProperties);
        var sagaDefaultSettings = sagaPropertiesMapper.map(sagaDefaultProperties);

        sagaRegisterSettingsService.registerDefaultSagaSettings(sagaDefaultSettings);

        sagaPropertiesGroupOpt
            .map(DistributedSagaProperties.SagaPropertiesGroup::getSagaPropertiesGroup)
            .map(Map::entrySet)
            .stream()
            .flatMap(Collection::stream)
            .forEach(entry -> {
                var sagaCustomProperties = sagaPropertiesMerger.merge(sagaDefaultProperties, entry.getValue());
                var sagaCustomSettings = sagaPropertiesMapper.map(sagaCustomProperties);

                sagaRegisterSettingsService.registerSagaSettings(
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
    @Override
    public SagaMethodSettings buildSagaMethodSettings(Method method,
                                                      @Nullable String suffix,
                                                      @Nullable DistributedSagaProperties.SagaMethodPropertiesGroup sagaMethodPropertiesGroup) {
        var sagaMethodPropertiesGroupOpt = Optional.ofNullable(sagaMethodPropertiesGroup);

        var sagaMethodDefaultCodeProperties = sagaMethodPropertiesMapper.map(SagaMethodSettings.buildDefault());
        var sagaMethodCustomCodeProperties = fillCustomProperties(method);

        var sagaMethodDefaultConfProperties = sagaMethodPropertiesGroupOpt
            .map(DistributedSagaProperties.SagaMethodPropertiesGroup::getDefaultSagaMethodProperties)
            .orElse(null);
        var sagaMethodCustomConfProperties = sagaMethodPropertiesGroupOpt
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
        fillNoRetryFor(method, taskProperties);
        return taskProperties;
    }

    private void fillExecutionGuarantees(Method method, DistributedSagaProperties.SagaMethodProperties sagaMethodProperties) {
        com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, Transactional.class)
            .ifPresent(executionGuarantees ->
                sagaMethodProperties.setExecutionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
            );
    }

    private void fillNoRetryFor(Method method, DistributedSagaProperties.SagaMethodProperties taskProperties) {
        com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, SagaMethod.class)
            .ifPresent(sagaMethod -> taskProperties.setNoRetryFor(Lists.newArrayList(sagaMethod.noRetryFor())));
    }
}
