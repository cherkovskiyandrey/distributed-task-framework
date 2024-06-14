package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaResultEntity;

public interface ExtendedSagaResultRepository {

    SagaResultEntity saveOrUpdate(SagaResultEntity sagaResultEntity);
}
