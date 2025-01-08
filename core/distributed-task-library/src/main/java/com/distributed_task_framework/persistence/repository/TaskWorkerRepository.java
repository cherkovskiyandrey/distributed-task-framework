package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

@Repository
public interface TaskWorkerRepository {

    Collection<TaskEntity> getNextTasks(UUID workerId, Set<TaskId> skippedTasks, int maxSize);

    Set<TaskId> filterCanceled(Set<TaskId> taskIds);
}
