package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.autoconfigure.mapper.RetrySettingsMerger;
import com.distributed_task_framework.saga.DistributedSagaProperties.SagaMethodProperties;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    uses = RetrySettingsMerger.class,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface SagaMethodPropertiesMerger {

    SagaMethodProperties merge(@MappingTarget SagaMethodProperties defaultSagaMethodProperties,
                               @Nullable SagaMethodProperties sagaMethodProperties);
}
