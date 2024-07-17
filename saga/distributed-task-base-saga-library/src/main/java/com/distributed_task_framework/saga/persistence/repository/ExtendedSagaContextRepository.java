package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface ExtendedSagaContextRepository {

    SagaContextEntity saveOrUpdate(SagaContextEntity sagaContextEntity);

    List<UUID> removeHanging(Duration delay);

    List<UUID> removeExpired(Duration delay);
}
