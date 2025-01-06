package com.distributed_task_framework.saga.mappers;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(
    componentModel = "spring",
    unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface SagaMethodPropertiesMapper {

    //DistributedTaskProperties.TaskProperties map(SagaConfiguration.SagaMethodProperties defaultSagaMethodProperties);
}
