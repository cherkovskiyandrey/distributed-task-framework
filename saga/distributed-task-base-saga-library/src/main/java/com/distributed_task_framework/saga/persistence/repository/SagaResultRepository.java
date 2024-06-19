package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaResultEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;

public interface SagaResultRepository extends CrudRepository<SagaResultEntity, UUID>, ExtendedSagaResultRepository {
}
