package com.distributed_task_framework.autoconfigure.persistence.repository;

import com.distributed_task_framework.autoconfigure.persistence.entity.TestDataEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;

public interface TestDataRepository extends CrudRepository<TestDataEntity, UUID> {
}
