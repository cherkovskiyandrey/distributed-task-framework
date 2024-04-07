package com.distributed_task_framework.test_service.persistence.repository;

import com.distributed_task_framework.test_service.persistence.entities.TestDataEntity;
import org.springframework.data.repository.CrudRepository;

public interface TestDataRepository extends CrudRepository<TestDataEntity, Long> {
}
