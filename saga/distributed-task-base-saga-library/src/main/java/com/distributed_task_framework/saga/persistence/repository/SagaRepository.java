package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface SagaRepository extends CrudRepository<SagaEntity, UUID>, ExtendedSagaRepository {

    //language=postgresql
    @Query("""
        SELECT * FROM _____dtf_saga WHERE saga_id = :sagaId::uuid
        """)
    @Lock(LockMode.PESSIMISTIC_WRITE)
    Optional<SagaEntity> findByIdIfExists(@Param("sagaId") UUID sagaId);
}
