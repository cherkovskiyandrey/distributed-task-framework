package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.CapabilityEntity;

import java.util.Set;

public interface CapabilityExtendedRepository {
    void saveOrUpdateBatch(Set<CapabilityEntity> currentCapabilities);
}
