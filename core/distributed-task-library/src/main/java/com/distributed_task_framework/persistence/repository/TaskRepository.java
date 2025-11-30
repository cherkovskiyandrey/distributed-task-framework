package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface TaskRepository extends
        CrudRepository<TaskEntity, UUID>,
        TaskExtendedRepository,
        TaskCommandRepository,
        TaskVirtualQueueBasePlannerRepository,
        VirtualQueueManagerPlannerRepository,
        PartitionTrackerRepository,
        TaskWorkerRepository,
        TaskStatRepository {
}
