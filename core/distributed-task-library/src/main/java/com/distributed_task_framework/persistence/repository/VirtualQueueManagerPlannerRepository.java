package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.exception.BatchUpdateException;
import com.distributed_task_framework.model.AffinityGroupAndAffinity;
import com.distributed_task_framework.model.AffinityGroupStat;
import com.distributed_task_framework.model.AffinityGroupWrapper;
import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.persistence.entity.IdVersionWithAffinityEntity;
import com.distributed_task_framework.persistence.entity.IdVersionWithVirtualQueue;
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

    List<IdVersionWithVirtualQueue> getTasksFromNew(Set<AffinityGroupStat> affinityGroupStats);

    List<ShortTaskEntity> moveNewToReadyAndParked(List<IdVersionWithVirtualQueue> idVersionWithVirtualQueues);

    Set<IdVersionWithAffinityEntity> readyToHardDelete(int batchSize);

    List<IdVersionEntity> readyToMoveFromParkedToReady(Collection<AffinityGroupAndAffinity> affinityGroupAndAffinities);

    List<ShortTaskEntity> moveParkedToReady(Collection<IdVersionEntity> idVersionEntities);

    @VisibleForTesting
    int countOfTasksInVirtualQueue(VirtualQueue virtualQueue);

    /**
     * @param taskEntity
     * @throws OptimisticLockException
     */
    TaskEntity softDelete(TaskEntity taskEntity);

    /**
     *
     * @param taskEntities
     * @throws BatchUpdateException
     */
    void softDeleteAll(Collection<TaskEntity> taskEntities);
}
