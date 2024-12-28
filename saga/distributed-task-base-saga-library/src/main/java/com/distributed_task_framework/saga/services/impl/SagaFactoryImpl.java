package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.services.SagaFlowEntryPoint;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.services.SagaFactory;
import com.distributed_task_framework.saga.services.internal.SagaRegister;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.utils.StringUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

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
public class SagaFactoryImpl implements SagaFactory {
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    SagaHelper sagaHelper;

    @Override
    public SagaFlowEntryPoint create(String name) {
        StringUtils.requireNotBlank(name, "name");
        return SagaFlowEntryPointImpl.builder()
            .name(name)
            .transactionManager(transactionManager)
            .sagaRegister(sagaRegister)
            .distributedTaskService(distributedTaskService)
            .sagaManager(sagaManager)
            .sagaHelper(sagaHelper)
            .build();
    }

    @Override
    public SagaFlowEntryPoint createWithAffinity(String name, String affinityGroup, String affinity) {
        StringUtils.requireNotBlank(name, "name");
        StringUtils.requireNotBlank(affinityGroup, "affinityGroup");
        StringUtils.requireNotBlank(affinity, "affinity");
        return SagaFlowEntryPointImpl.builder()
            .name(name)
            .affinityGroup(affinityGroup)
            .affinity(affinity)
            .transactionManager(transactionManager)
            .sagaRegister(sagaRegister)
            .distributedTaskService(distributedTaskService)
            .sagaManager(sagaManager)
            .sagaHelper(sagaHelper)
            .build();
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
