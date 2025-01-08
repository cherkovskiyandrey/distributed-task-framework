package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
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
public interface DistributedTaskPropertiesMerger {

    DistributedTaskProperties.TaskProperties merge(@MappingTarget DistributedTaskProperties.TaskProperties defaultTaskProperties,
                                                   @Nullable DistributedTaskProperties.TaskProperties taskProperties);
}
