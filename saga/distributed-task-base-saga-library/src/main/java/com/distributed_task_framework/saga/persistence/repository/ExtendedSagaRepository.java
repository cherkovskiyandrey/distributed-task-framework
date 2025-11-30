package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat;
import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.entities.ShortSagaEntity;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ExtendedSagaRepository {

    @SuppressWarnings("UnusedReturnValue")
    SagaEntity saveOrUpdate(SagaEntity sagaEntity);

    Optional<ShortSagaEntity> findShortById(UUID sagaId);

    Optional<Boolean> isCompleted(UUID sagaId);

    Optional<Boolean> isCanceled(UUID sagaId);

    List<SagaEntity> findExpired(Integer batchSize);

    List<ShortSagaEntity> removeCompleted();

    void removeAll(List<UUID> sagaIds);

    List<AggregatedSagaStat> getAggregatedSagaStat(Duration timeToClean);

    List<AggregatedSagaStat> getAggregatedTopNSagaStat(Integer tonNSize, Duration timeToClean);
}
