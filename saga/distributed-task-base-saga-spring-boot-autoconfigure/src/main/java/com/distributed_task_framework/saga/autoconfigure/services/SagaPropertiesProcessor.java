package com.distributed_task_framework.saga.autoconfigure.services;

import com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties;
import com.distributed_task_framework.saga.services.SagaRegisterSettingsService;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import jakarta.annotation.Nullable;

import java.lang.reflect.Method;

public interface SagaPropertiesProcessor {

    SagaCommonSettings buildSagaCommonSettings(@Nullable DistributedSagaProperties.Common common);

    void registerConfiguredSagas(SagaRegisterSettingsService sagaRegisterSettingsService,
                                 @Nullable DistributedSagaProperties.SagaPropertiesGroup sagaPropertiesGroup);

    SagaMethodSettings buildSagaMethodSettings(Method method,
                                               @Nullable String suffix,
                                               @Nullable DistributedSagaProperties.SagaMethodPropertiesGroup sagaMethodPropertiesGroup);
}
