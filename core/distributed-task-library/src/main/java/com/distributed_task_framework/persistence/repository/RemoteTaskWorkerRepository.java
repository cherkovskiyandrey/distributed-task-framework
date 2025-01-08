package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.RemoteTaskWorkerEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface RemoteTaskWorkerRepository extends CrudRepository<RemoteTaskWorkerEntity, UUID> {

    Optional<RemoteTaskWorkerEntity> findByAppName(String appName);
}
