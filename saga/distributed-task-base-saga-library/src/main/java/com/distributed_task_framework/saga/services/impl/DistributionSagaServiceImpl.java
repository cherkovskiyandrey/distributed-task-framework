package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaFlowEntryPoint;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.saga.functions.SagaFunction;
import com.distributed_task_framework.saga.services.SagaRegisterService;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.utils.StringUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.UUID;


/**
 * 0. Проверить и если нужно переписать на новый механизм обнаружения методов без явного вызова лабды (-)
 * 1. Покрыть абсолютно все тестами (-)
 * 2. Дореализовать все остальные методы (-)
 * 3. Написать более честный example используя мок сервис для эмуляции взаимодействия с внешним сервисом (-)
 * 4. Подумать над перф тестом (-)
 * 5. Временно ронять контескт если кол-во тасок больше чем dtf может за раз запланировать (-) ???
 * 6. Поддержка map/reduce через расширение контекста SagaEmbeddedPipelineContext и динамического достраивания DAG-а из самих тасок
 * (вернее во время старта - достаточно создать 1 уровень map/reduce, дальше из тасок динамически достраивать DAG) (-)
 * 7. Think about exactly once for remote http call - could be possible only based on remote tasks (-)
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class DistributionSagaServiceImpl implements DistributionSagaService {
    PlatformTransactionManager transactionManager;
    SagaResolver sagaResolver;
    SagaRegisterService sagaRegisterService;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    SagaHelper sagaHelper;

    @Override
    public void registerSagaSettings(String name, SagaSettings sagaSettings) {
        sagaRegisterService.registerSagaSettings(name, sagaSettings);
    }

    @Override
    public void registerDefaultSagaSettings(SagaSettings sagaSettings) {
        sagaRegisterService.registerDefaultSagaSettings(sagaSettings);
    }

    @Override
    public SagaSettings getSagaSettings(String name) {
        return sagaRegisterService.getSagaSettings(name);
    }

    @Override
    public void unregisterSagaSettings(String name) {
        sagaRegisterService.unregisterSagaSettings(name);
    }

    @Override
    public void registerSagaMethod(String name, Method method, Object object, SagaMethodSettings sagaMethodSettings) {
        sagaRegisterService.registerSagaMethod(name, method, object, sagaMethodSettings);
    }

    @Override
    public <T, R> void registerSagaMethod(String name,
                                          SagaFunction<T, R> methodRef,
                                          Object object,
                                          SagaMethodSettings sagaMethodSettings) {
        sagaRegisterService.registerSagaMethod(name, methodRef, object, sagaMethodSettings);
    }

    @Override
    public void registerSagaRevertMethod(String name,
                                         Method method,
                                         Object object,
                                         SagaMethodSettings sagaMethodSettings) {
        sagaRegisterService.registerSagaRevertMethod(name, method, object, sagaMethodSettings);
    }

    @Override
    public void unregisterSagaMethod(String name) {
        sagaRegisterService.unregisterSagaMethod(name);
    }

    @Override
    public SagaFlowEntryPoint create(String name, SagaSettings sagaSettings) {
        log.info("create(): name=[{}], sagaSettings=[{}]", name, sagaSettings);
        StringUtils.requireNotBlank(name, "name");
        Objects.requireNonNull(sagaSettings, "sagaSettings");
        return SagaFlowEntryPointImpl.builder()
            .name(name)
            .sagaSettings(sagaSettings)
            .transactionManager(transactionManager)
            .sagaResolver(sagaResolver)
            .distributedTaskService(distributedTaskService)
            .sagaManager(sagaManager)
            .sagaHelper(sagaHelper)
            .build();
    }

    @Override
    public SagaFlowEntryPoint create(String name) {
        return create(name, sagaRegisterService.getSagaSettings(name));
    }

    @Override
    public SagaFlowEntryPoint createWithAffinity(String name, String affinityGroup, String affinity, SagaSettings sagaSettings) {
        log.info(
            "createWithAffinity(): name=[{}], affinityGroup=[{}], affinity=[{}], sagaSettings=[{}]",
            name,
            affinityGroup,
            affinity,
            sagaSettings
        );
        StringUtils.requireNotBlank(name, "name");
        StringUtils.requireNotBlank(affinityGroup, "affinityGroup");
        StringUtils.requireNotBlank(affinity, "affinity");
        Objects.requireNonNull(sagaSettings, "sagaSettings");
        return SagaFlowEntryPointImpl.builder()
            .name(name)
            .sagaSettings(sagaSettings)
            .affinityGroup(affinityGroup)
            .affinity(affinity)
            .transactionManager(transactionManager)
            .sagaResolver(sagaResolver)
            .distributedTaskService(distributedTaskService)
            .sagaManager(sagaManager)
            .sagaHelper(sagaHelper)
            .build();
    }

    @Override
    public SagaFlowEntryPoint createWithAffinity(String name, String affinityGroup, String affinity) {
        return createWithAffinity(name, affinityGroup, affinity, sagaRegisterService.getSagaSettings(name));
    }

    @Override
    public <OUTPUT> SagaFlow<OUTPUT> getFlow(UUID trackId, Class<OUTPUT> trackingClass) {
        return SagaFlowImpl.<OUTPUT>builder()
            .distributedTaskService(distributedTaskService)
            .sagaManager(sagaManager)
            .sagaId(trackId)
            .resultType(trackingClass)
            .build();
    }

    @Override
    public SagaFlowWithoutResult getFlow(UUID trackId) {
        //todo
        throw new UnsupportedOperationException();
    }
}
