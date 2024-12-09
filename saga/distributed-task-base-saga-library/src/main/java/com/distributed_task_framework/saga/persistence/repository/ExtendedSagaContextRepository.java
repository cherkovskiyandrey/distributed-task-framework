package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaEntity;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface ExtendedSagaContextRepository {

    @SuppressWarnings("UnusedReturnValue")
    SagaEntity saveOrUpdate(SagaEntity sagaEntity);

    List<SagaEntity> findExpired();

    List<UUID> removeCompleted(Duration delay);

    void removeAll(List<UUID> sagaIds);
}
