package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.entities.ShortSagaEntity;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ExtendedSagaContextRepository {

    @SuppressWarnings("UnusedReturnValue")
    SagaEntity saveOrUpdate(SagaEntity sagaEntity);

    Optional<ShortSagaEntity> findShortById(UUID sagaId);

    Optional<Boolean> isCompleted(UUID sagaId);

    Optional<Boolean> isCanceled(UUID sagaId);

    List<SagaEntity> findExpired();

    List<UUID> removeCompleted(Duration delay);

    void removeAll(List<UUID> sagaIds);
}
