package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.settings.CommonSettings;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface StatSettingsMerger {

    default CommonSettings.StatSettings merge(@MappingTarget CommonSettings.StatSettings defaultStatistics,
                                              @Nullable DistributedTaskProperties.Statistics statistics) {
        if (statistics == null) {
            return defaultStatistics;
        }
        
        DistributedTaskProperties.Statistics defaultPropertiesRegistry = map(defaultStatistics);
        DistributedTaskProperties.Statistics mergedRegistry = merge(defaultPropertiesRegistry, statistics);
        return map(mergedRegistry);
    }

    DistributedTaskProperties.Statistics map(CommonSettings.StatSettings defaultStatistics);

    DistributedTaskProperties.Statistics merge(@MappingTarget DistributedTaskProperties.Statistics defaultStatistics,
                                               DistributedTaskProperties.Statistics statistics);

    CommonSettings.StatSettings map(DistributedTaskProperties.Statistics mergedStatistics);
}
