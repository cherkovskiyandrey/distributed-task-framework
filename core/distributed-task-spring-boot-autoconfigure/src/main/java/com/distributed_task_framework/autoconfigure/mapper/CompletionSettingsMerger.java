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
public interface CompletionSettingsMerger {

    default CommonSettings.CompletionSettings merge(@MappingTarget CommonSettings.CompletionSettings defaultCompletionSettings,
                                                    @Nullable DistributedTaskProperties.Completion completion) {
        if (completion == null) {
            return defaultCompletionSettings;
        }
        DistributedTaskProperties.Completion defualtPropertiesCompletion = map(defaultCompletionSettings);
        DistributedTaskProperties.Completion mergedCompletion = merge(defualtPropertiesCompletion, completion);
        return map(mergedCompletion);
    }

    DistributedTaskProperties.Completion map(CommonSettings.CompletionSettings defaultCompletionSettings);

    DistributedTaskProperties.Completion merge(@MappingTarget DistributedTaskProperties.Completion defualtPropertiesCompletion,
                                               DistributedTaskProperties.Completion completion);

    CommonSettings.CompletionSettings map(DistributedTaskProperties.Completion mergedCompletion);
}
