package com.distributed_task_framework.saga.test_service.persistence.repository;

import com.distributed_task_framework.saga.test_service.persistence.entities.Audit;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AuditRepository extends CrudRepository<Audit, Long> {
}
