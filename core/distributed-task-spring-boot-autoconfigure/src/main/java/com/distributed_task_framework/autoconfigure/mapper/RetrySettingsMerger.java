package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.settings.Retry;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;


@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface RetrySettingsMerger {

    default Retry merge(@MappingTarget Retry defaultRetrySettings, DistributedTaskProperties.Retry retry) {
        if (retry == null) {
            return defaultRetrySettings.toBuilder()
                .build();
        }
        DistributedTaskProperties.Retry defaultRetryProperties = map(defaultRetrySettings);
        defaultRetryProperties = mergeInternal(defaultRetryProperties, retry);
        return map(defaultRetryProperties);
    }

    DistributedTaskProperties.Retry map(Retry defaultRetrySettings);

    DistributedTaskProperties.Retry mergeInternal(@MappingTarget DistributedTaskProperties.Retry defaultRetryProperties,
                                                  DistributedTaskProperties.Retry retry);

    DistributedTaskProperties.Fixed merge(@MappingTarget DistributedTaskProperties.Fixed defaultBackoff,
                                          DistributedTaskProperties.Fixed backoff);

    DistributedTaskProperties.Backoff merge(@MappingTarget DistributedTaskProperties.Backoff defaultBackoff,
                                            DistributedTaskProperties.Backoff backoff);

    Retry map(DistributedTaskProperties.Retry defaultRetryProperties);
}
