package com.distributed_task_framework.saga.autoconfigure.mappers;

import com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface SagaStatPropertiesMerger {

    DistributedSagaProperties.SagaStatProperties merge(@MappingTarget DistributedSagaProperties.SagaStatProperties defaultSagaStatProperties,
                                                       @Nullable DistributedSagaProperties.SagaStatProperties sagaStatProperties);
}
