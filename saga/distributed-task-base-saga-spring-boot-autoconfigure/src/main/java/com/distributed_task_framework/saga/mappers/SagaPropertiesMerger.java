package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.saga.DistributedSagaProperties.SagaProperties;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface SagaPropertiesMerger {

    SagaProperties merge(@MappingTarget SagaProperties defaultCodeProperties,
                         @Nullable SagaProperties sagaConfProperties);
}
