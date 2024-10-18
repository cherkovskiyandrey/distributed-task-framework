package com.distributed_task_framework.saga.persistence.repository;

import com.distributed_task_framework.saga.persistence.entities.DlsSagaContextEntity;

import java.util.Collection;

public interface ExtendedDlsSagaContextRepository {

    /**
     * Batch save or update.
     *
     * @param dlsSagaContextEntities
     * @return only affected entities
     */
    @SuppressWarnings("UnusedReturnValue")
    Collection<DlsSagaContextEntity> saveOrUpdateAll(Collection<DlsSagaContextEntity> dlsSagaContextEntities);
}
