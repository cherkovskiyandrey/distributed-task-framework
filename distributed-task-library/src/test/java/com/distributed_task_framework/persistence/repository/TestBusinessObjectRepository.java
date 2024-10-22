package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.repository.entity.TestBusinessObjectEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface TestBusinessObjectRepository extends CrudRepository<TestBusinessObjectEntity, UUID> {
}
