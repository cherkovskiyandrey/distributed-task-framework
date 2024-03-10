package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.PartitionEntity;

import java.util.Collection;

public interface PartitionExtendedRepository {

    @SuppressWarnings("UnusedReturnValue")
    Collection<PartitionEntity> saveOrUpdateBatch(Collection<PartitionEntity> taskNameEntities);

    Collection<PartitionEntity> filterExisted(Collection<PartitionEntity> entities);

    Collection<PartitionEntity> findAllBeforeOrIn(Long maxTimeBucket);

    void compactInTimeWindow(Long timeBucket);

    void deleteBatch(Collection<PartitionEntity> toRemove);
}
