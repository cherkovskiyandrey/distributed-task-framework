package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.Partition;

import java.util.Set;

public interface PartitionTrackerRepository {

    Set<Partition> activePartitions();

    Set<Partition> filterInReadyVirtualQueue(Set<Partition> entities);
}
