package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.services.SagaManager;
import com.distributed_task_framework.saga.services.SagaEntryPoint;
import com.distributed_task_framework.saga.services.SagaFlow;
import com.distributed_task_framework.saga.services.SagaFlowWithoutResult;
import com.distributed_task_framework.saga.services.SagaProcessor;
import com.distributed_task_framework.saga.services.SagaRegister;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.utils.StringUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.UUID;



/**
 * 1. Реализовать возможность отмены саги - который будет останавливать сагу на текущем моменте
 *  и запускать ролбэк асинхронный (-)
 * 2. Дореализовать все остальные методы (-)
 * 2. todo: now it will not work, because DTF doesn't allow to create tasks form exception (+)
 * //          ^^^^ - fixed, need to be covered by tests (-)
 * 3. Покрыть абсолютно все тестами (-)
 * 4. Написать более честный example используя мок сервис для эмуляции взаимодействия с внешним сервисом (-)
 * 5. Подумать над перф тестом (-)
 * 6. Временно ронять контескт если кол-во тасок больше чем dtf может за раз запланировать (-) ???
 * 7. Поддержка map/reduce через расширение контекста SagaEmbeddedPipelineContext и динамического достраивания DAG-а из самих тасок
 *  (вернее во время старта - достаточно создать 1 уровень map/reduce, дальше из тасок динамически достраивать DAG) (-)
 * 8. Think about exactly once for remote http call - could be possible only based on remote tasks (-)
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaProcessorImpl implements SagaProcessor {
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaManager sagaManager;
    SagaHelper sagaHelper;

    @Override
    public SagaEntryPoint create(String name) {
        StringUtils.requireNotBlank(name, "name");
        return SagaEntryPointImpl.builder()
                .userName(name)
                .transactionManager(transactionManager)
                .sagaRegister(sagaRegister)
                .distributedTaskService(distributedTaskService)
                .sagaManager(sagaManager)
                .sagaHelper(sagaHelper)
                .build();
    }

    @Override
    public SagaEntryPoint createWithAffinity(String name, String affinityGroup, String affinity) {
        StringUtils.requireNotBlank(name, "name");
        StringUtils.requireNotBlank(affinityGroup, "affinityGroup");
        StringUtils.requireNotBlank(affinity, "affinity");
        return SagaEntryPointImpl.builder()
                .userName(name)
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
