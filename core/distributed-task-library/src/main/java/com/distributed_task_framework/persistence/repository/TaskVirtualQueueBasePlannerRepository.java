package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.NodeTaskActivity;
import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.PartitionStat;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Repository
public interface TaskVirtualQueueBasePlannerRepository {

    List<NodeTaskActivity> currentAssignedTaskStat(Set<UUID> knownNodes, Set<String> knownTaskNames);

    Set<PartitionStat> findPartitionStatToPlan(Set<UUID> knownNodes,
                                               Set<Partition> entities,
                                               int limit);

    Collection<ShortTaskEntity> loadTasksToPlan(Set<UUID> knownNodes,
                                                Map<Partition, Integer> partitionToLimits);
}
