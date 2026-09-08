package com.distributed_task_framework.mapper;

import com.distributed_task_framework.persistence.entity.IdVersionEntity;
import com.distributed_task_framework.persistence.entity.IdVersionWithAffinityEntity;
import com.distributed_task_framework.persistence.entity.IdVersionWithVirtualQueue;
import com.distributed_task_framework.persistence.entity.TaskEntity;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

import java.util.Collection;
import java.util.Set;

@Mapper(
    unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface IdVersionMapper {

    Collection<IdVersionEntity> map(Set<IdVersionWithAffinityEntity> idVersionWithAffinityEntities);

    IdVersionEntity mapToIdVersion(TaskEntity taskEntity);

    IdVersionWithAffinityEntity map(TaskEntity taskEntity);

    IdVersionWithVirtualQueue mapToIdVersionWithVirtualQueue(TaskEntity taskEntity);
}
