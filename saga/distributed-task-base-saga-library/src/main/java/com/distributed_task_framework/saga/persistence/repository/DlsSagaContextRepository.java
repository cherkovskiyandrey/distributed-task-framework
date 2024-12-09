package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.DlsSagaEntity;
import org.springframework.data.repository.CrudRepository;

import java.util.UUID;

public interface DlsSagaContextRepository extends CrudRepository<DlsSagaEntity, UUID>, ExtendedDlsSagaContextRepository {
}
