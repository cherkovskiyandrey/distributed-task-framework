package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.PlannerEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface PlannerRepository extends CrudRepository<PlannerEntity, UUID> {

    Optional<PlannerEntity> findByGroupName(String groupName);
}
