package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import com.distributed_task_framework.persistence.entity.VirtualQueue;
import com.google.common.annotations.VisibleForTesting;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface VirtualQueueManagerPlannerRepository {

    Optional<LocalDateTime> maxCreatedDateInNewVirtualQueue();

    Set<AffinityGroupWrapper> affinityGroupsInNewVirtualQueue(LocalDateTime from, Duration overlap);

    Set<AffinityGroupStat> affinityGroupInNewVirtualQueueStat(Set<AffinityGroupWrapper> knownAffinityGroups,
                                                              int affinityGroupLimit);

    List<ShortTaskEntity> moveNewToReady(Set<AffinityGroupStat> affinityGroupStats);

    Set<IdVersionEntity> readyToHardDelete(int batchSize);

    List<ShortTaskEntity> moveParkedToReady(Collection<IdVersionEntity> idVersionEntities);

    @VisibleForTesting
    int countOfTasksInVirtualQueue(VirtualQueue virtualQueue);

    /**
     * @param taskEntity
     * @throws OptimisticLockException
     */
    void softDelete(TaskEntity taskEntity);
}
