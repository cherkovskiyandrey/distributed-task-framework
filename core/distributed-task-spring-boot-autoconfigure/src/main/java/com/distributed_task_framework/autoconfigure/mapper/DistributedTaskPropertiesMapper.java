package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.settings.TaskSettings;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface DistributedTaskPropertiesMapper {

    TaskSettings map(DistributedTaskProperties.TaskProperties taskProperties);

    DistributedTaskProperties.TaskProperties map(TaskSettings taskSettings);
}
