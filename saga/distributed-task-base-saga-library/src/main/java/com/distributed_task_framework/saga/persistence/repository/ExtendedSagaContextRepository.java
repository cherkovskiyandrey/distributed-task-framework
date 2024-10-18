package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface ExtendedSagaContextRepository {

    @SuppressWarnings("UnusedReturnValue")
    SagaContextEntity saveOrUpdate(SagaContextEntity sagaContextEntity);

    List<SagaContextEntity> findExpired();

    List<UUID> removeCompleted(Duration delay);

    void removeAll(List<UUID> sagaIds);
}
