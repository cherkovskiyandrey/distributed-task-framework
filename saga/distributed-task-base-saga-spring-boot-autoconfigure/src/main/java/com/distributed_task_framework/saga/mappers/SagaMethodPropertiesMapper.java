package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.saga.DistributedSagaProperties;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface SagaMethodPropertiesMapper {

    DistributedSagaProperties.SagaMethodProperties map(SagaMethodSettings sagaMethodSettings);

    SagaMethodSettings map(DistributedSagaProperties.SagaMethodProperties sagaMethodProperties);
}
