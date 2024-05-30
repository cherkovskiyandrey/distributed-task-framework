package com.distributed_task_framework.mapper;

import com.distributed_task_framework.model.NodeLoading;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;

@Mapper(
        unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface NodeStateMapper {

    NodeLoading fromEntity(NodeStateEntity nodeStateEntity);
}
