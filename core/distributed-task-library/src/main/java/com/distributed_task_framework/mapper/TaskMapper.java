package com.distributed_task_framework.mapper;

import com.distributed_task_framework.model.Partition;
import com.distributed_task_framework.model.TaskId;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import com.distributed_task_framework.persistence.entity.DltEntity;
import com.distributed_task_framework.persistence.entity.ShortTaskEntity;
import com.distributed_task_framework.persistence.entity.TaskEntity;

import java.util.Collection;
import java.util.List;

@Mapper(
        unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface TaskMapper {

    TaskId map(TaskEntity taskEntity, String appName);

    @Mapping(target = "version", ignore = true)
    DltEntity mapToDlt(TaskEntity taskEntity);

    ShortTaskEntity mapToShort(TaskEntity taskEntity);

    List<ShortTaskEntity> mapToShort(Collection<TaskEntity> taskEntity);

    Partition mapToPartition(ShortTaskEntity shortTaskEntity);

    Partition mapToPartition(TaskEntity taskEntity);
}
