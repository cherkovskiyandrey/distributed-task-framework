package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.settings.CommonSettings;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;
import org.springframework.beans.factory.annotation.Autowired;

@Mapper(
    uses = {
        CompletionSettingsMerger.class,
        RegistrySettingsMerger.class,
        PlannerSettingsMerger.class,
        WorkerManagerSettingsMerger.class,
        StatSettingsMerger.class,
        DeliveryManagerSettingsMerger.class
    },
    componentModel = MappingConstants.ComponentModel.SPRING,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
@FieldDefaults(level = AccessLevel.PRIVATE)
public abstract class CommonSettingsMerger {
    @Autowired
    CompletionSettingsMerger completionSettingsMerger;
    @Autowired
    RegistrySettingsMerger registrySettingsMerger;
    @Autowired
    PlannerSettingsMerger plannerSettingsMerger;
    @Autowired
    WorkerManagerSettingsMerger workerManagerSettingsMerger;
    @Autowired
    StatSettingsMerger statSettingsMerger;
    @Autowired
    DeliveryManagerSettingsMerger deliveryManagerSettingsMerger;

    public CommonSettings merge(@MappingTarget CommonSettings commonSettingsTo,
                                @Nullable DistributedTaskProperties.Common commonPropertiesFrom) {
        if (commonPropertiesFrom == null) {
            return commonSettingsTo;
        }

        var completionSettings = completionSettingsMerger.merge(
            commonSettingsTo.getCompletionSettings().toBuilder().build(),
            commonPropertiesFrom.getCompletion()
        );

        var registrySettings = registrySettingsMerger.merge(
            commonSettingsTo.getRegistrySettings().toBuilder().build(),
            commonPropertiesFrom.getRegistry());

        var plannerSettings = plannerSettingsMerger.merge(
            commonSettingsTo.getPlannerSettings(),
            commonPropertiesFrom.getPlanner()
        );

        var workerManagerSettings = workerManagerSettingsMerger.merge(
            commonSettingsTo.getWorkerManagerSettings(),
            commonPropertiesFrom.getWorkerManager());

        var statSettings = statSettingsMerger.merge(
            commonSettingsTo.getStatSettings(),
            commonPropertiesFrom.getStatistics());

        var deliveryManagerSettings = deliveryManagerSettingsMerger.merge(
            commonSettingsTo.getDeliveryManagerSettings(),
            commonPropertiesFrom.getDeliveryManager());

        return mergeInternal(commonSettingsTo, commonPropertiesFrom).toBuilder()
            .completionSettings(completionSettings)
            .registrySettings(registrySettings)
            .plannerSettings(plannerSettings)
            .workerManagerSettings(workerManagerSettings)
            .statSettings(statSettings)
            .deliveryManagerSettings(deliveryManagerSettings)
            .build();
    }

    public CommonSettings mergeInternal(@MappingTarget CommonSettings commonSettings,
                                        DistributedTaskProperties.Common common) {
        DistributedTaskProperties.Common defaultCommon = map(commonSettings);
        DistributedTaskProperties.Common mergedSettings = merge(defaultCommon, common);
        return map(mergedSettings);
    }

    @Mapping(target = "completion", ignore = true)
    @Mapping(target = "registry", ignore = true)
    @Mapping(target = "planner", ignore = true)
    @Mapping(target = "workerManager", ignore = true)
    @Mapping(target = "statistics", ignore = true)
    @Mapping(target = "deliveryManager", ignore = true)
    public abstract DistributedTaskProperties.Common map(CommonSettings commonSettings);

    @Mapping(target = "completion", ignore = true)
    @Mapping(target = "registry", ignore = true)
    @Mapping(target = "planner", ignore = true)
    @Mapping(target = "workerManager", ignore = true)
    @Mapping(target = "statistics", ignore = true)
    @Mapping(target = "deliveryManager", ignore = true)
    public abstract DistributedTaskProperties.Common merge(@MappingTarget DistributedTaskProperties.Common defaultCommon,
                                                           DistributedTaskProperties.Common common);

    @Mapping(target = "completionSettings", ignore = true)
    @Mapping(target = "registrySettings", ignore = true)
    @Mapping(target = "plannerSettings", ignore = true)
    @Mapping(target = "workerManagerSettings", ignore = true)
    @Mapping(target = "statSettings", ignore = true)
    @Mapping(target = "deliveryManagerSettings", ignore = true)
    public abstract CommonSettings map(DistributedTaskProperties.Common mergedSettings);
}
