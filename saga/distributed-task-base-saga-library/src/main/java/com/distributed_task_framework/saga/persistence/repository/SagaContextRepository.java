package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface SagaContextRepository extends CrudRepository<SagaContextEntity, UUID>, ExtendedSagaContextRepository {

    //language=postgresql
    @Query("""
        SELECT
            CASE
                WHEN (
                        SELECT TRUE
                        FROM _____dtf_saga_context
                        WHERE
                        (completed_date_utc IS NOT NULL AND saga_id = :sagaId::uuid)
                        OR NOT EXISTS (
                            SELECT 1
                            FROM _____dtf_saga_context
                            WHERE saga_id = :sagaId::uuid
                        )
                    )
                    THEN TRUE
                ELSE FALSE
            END
        """)
    boolean isCompleted(@Param("sagaId") UUID sagaId);
}
