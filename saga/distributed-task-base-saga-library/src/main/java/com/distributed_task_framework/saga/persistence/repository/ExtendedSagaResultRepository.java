package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaResultEntity;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface ExtendedSagaResultRepository {

    SagaResultEntity saveOrUpdate(SagaResultEntity sagaResultEntity);

    List<UUID> removeExpiredEmptyResults(Duration delay);

    List<UUID> removeExpiredResults(Duration delay);
}
