package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.DlsSagaContextEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;

public interface DlsSagaContextRepository extends CrudRepository<DlsSagaContextEntity, UUID>, ExtendedDlsSagaContextRepository {
}
