package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;

public interface SagaContextRepository extends CrudRepository<SagaContextEntity, UUID>, ExtendedSagaContextRepository {
}
