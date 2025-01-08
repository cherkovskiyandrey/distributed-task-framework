package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.saga.DistributedSagaProperties;
import com.distributed_task_framework.saga.settings.SagaSettings;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface SagaPropertiesMapper {

    DistributedSagaProperties.SagaProperties map(SagaSettings sagaSettings);

    SagaSettings map(DistributedSagaProperties.SagaProperties sagaProperties);
}
