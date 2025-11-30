package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.DlcEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface DlcRepository extends CrudRepository<DlcEntity, UUID> {
}
