package com.distributed_task_framework.saga.autoconfigure.test_data.services.impl;

import com.distributed_task_framework.saga.autoconfigure.test_data.services.InternalSagaTestService;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import org.springframework.stereotype.Service;

@Service
public class InternalSagaTestServiceImpl extends InternalSagaBaseTestServiceImpl implements InternalSagaTestService {

    public InternalSagaTestServiceImpl(DistributionSagaService distributionSagaService) {
        super(distributionSagaService);
    }
}
