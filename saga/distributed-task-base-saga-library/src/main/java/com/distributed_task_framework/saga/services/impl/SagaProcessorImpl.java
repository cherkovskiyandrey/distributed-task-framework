package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.services.SagaContextService;
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

@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaProcessorImpl implements SagaProcessor {
    PlatformTransactionManager transactionManager;
    SagaRegister sagaRegister;
    DistributedTaskService distributedTaskService;
    SagaContextService sagaContextService;
    SagaHelper sagaHelper;

    @Override
    public SagaEntryPoint create(String name) {
        StringUtils.requireNotBlank(name, "name");
        return SagaEntryPointImpl.builder()
                .userName(name)
                .transactionManager(transactionManager)
                .sagaRegister(sagaRegister)
                .distributedTaskService(distributedTaskService)
                .sagaContextService(sagaContextService)
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
                .sagaContextService(sagaContextService)
                .sagaHelper(sagaHelper)
                .build();
    }

    @Override
    public <OUTPUT> SagaFlow<OUTPUT> getFlow(UUID trackId, Class<OUTPUT> trackingClass) {
        return SagaFlowImpl.<OUTPUT>builder()
                .distributedTaskService(distributedTaskService)
                .sagaContextService(sagaContextService)
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
