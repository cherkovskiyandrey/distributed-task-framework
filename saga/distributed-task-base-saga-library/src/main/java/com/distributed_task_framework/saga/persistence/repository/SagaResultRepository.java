package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.SagaResultEntity;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface SagaResultRepository extends CrudRepository<SagaResultEntity, UUID>, ExtendedSagaResultRepository {

    @Query("""
            DELETE FROM _____dtf_saga_result
            WHERE completed_date_utc IS NULL
            AND created_date_utc < now() - make_interval(secs => :delaySec)
            RETURNING saga_id
            """)
    List<UUID> removeExpiredEmptyResults(@Param("delaySec") long delaySec);

    @Query("""
            DELETE FROM _____dtf_saga_result
            WHERE completed_date_utc < now() - make_interval(secs => :delaySec)
            RETURNING saga_id
            """)
    List<UUID> removeExpiredResults(@Param("delaySec") long delaySec);
}
