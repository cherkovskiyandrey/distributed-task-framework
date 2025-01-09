package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.saga.DistributedSagaProperties;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface SagaCommonPropertiesMerger {

    DistributedSagaProperties.Common merge(@MappingTarget DistributedSagaProperties.Common defaultSagaCommonProperties,
                                           @Nullable DistributedSagaProperties.Common sagaCommonProperties);
}
