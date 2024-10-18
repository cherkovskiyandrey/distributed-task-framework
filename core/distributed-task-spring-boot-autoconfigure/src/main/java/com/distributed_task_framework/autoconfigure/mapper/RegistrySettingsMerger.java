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
public interface RegistrySettingsMerger {

    default CommonSettings.RegistrySettings merge(@MappingTarget CommonSettings.RegistrySettings defaultRegistrySettings,
                                                  @Nullable DistributedTaskProperties.Registry registry) {
        if (registry == null) {
            return defaultRegistrySettings;
        }

        DistributedTaskProperties.Registry defaultPropertiesRegistry = map(defaultRegistrySettings);
        DistributedTaskProperties.Registry mergedRegistry = merge(defaultPropertiesRegistry, registry);
        return map(mergedRegistry);
    }

    DistributedTaskProperties.Registry map(CommonSettings.RegistrySettings defaultRegistrySettings);

    DistributedTaskProperties.Registry merge(@MappingTarget DistributedTaskProperties.Registry defaultPropertiesRegistry,
                                             DistributedTaskProperties.Registry registry);

    CommonSettings.RegistrySettings map(DistributedTaskProperties.Registry mergedRegistry);
}
