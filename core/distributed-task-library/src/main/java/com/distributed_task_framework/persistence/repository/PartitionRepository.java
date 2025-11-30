package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.PartitionEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface PartitionRepository extends CrudRepository<PartitionEntity, UUID>, PartitionExtendedRepository {
}
