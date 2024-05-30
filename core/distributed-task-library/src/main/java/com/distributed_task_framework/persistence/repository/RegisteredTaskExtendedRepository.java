package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;

import java.util.Collection;

public interface RegisteredTaskExtendedRepository {

    void saveOrUpdateBatch(Collection<RegisteredTaskEntity> taskEntities);
}
