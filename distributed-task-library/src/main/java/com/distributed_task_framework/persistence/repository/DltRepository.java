package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.DltEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;

public interface DltRepository extends CrudRepository<DltEntity, UUID> {
}
