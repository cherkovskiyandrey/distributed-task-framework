package com.distributed_task_framework.saga.autoconfigure.services.impl;

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaSpecific;
import com.distributed_task_framework.saga.autoconfigure.services.SagaSpecificTestService;
import com.distributed_task_framework.saga.services.DistributionSagaService;

public class SagaSpecificTestServiceImpl extends InternalSagaBaseTestServiceImpl implements SagaSpecificTestService, SagaSpecific {
    private final String suffix;

    public SagaSpecificTestServiceImpl(DistributionSagaService distributionSagaService, String suffix) {
        super(distributionSagaService);
        this.suffix = suffix;
    }

    @Override
    public String suffix() {
        return suffix;
    }
}
