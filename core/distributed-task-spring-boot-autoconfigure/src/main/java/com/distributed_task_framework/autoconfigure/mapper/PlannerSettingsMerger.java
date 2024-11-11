package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.settings.CommonSettings;
import com.google.common.collect.ImmutableRangeMap;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;
import org.springframework.beans.factory.annotation.Autowired;

@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    uses = RangeDelayMapper.class,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public abstract class PlannerSettingsMerger {
    @Autowired
    private RangeDelayMapper rangeDelayMapper;

    public CommonSettings.PlannerSettings merge(@MappingTarget CommonSettings.PlannerSettings defaultPlannerSettings,
                                                @Nullable DistributedTaskProperties.Planner planner) {
        if (planner == null) {
            return defaultPlannerSettings;
        }

        defaultPlannerSettings = mergeInternal(defaultPlannerSettings, planner);
        var plannerBuilder = defaultPlannerSettings.toBuilder();
        if (!planner.getPollingDelay().isEmpty()) {
            ImmutableRangeMap<Integer, Integer> pollingDelay = rangeDelayMapper.mapRangeDelayProperty(planner.getPollingDelay());
            plannerBuilder.pollingDelay(pollingDelay);
        }
        return plannerBuilder.build();
    }

    public CommonSettings.PlannerSettings mergeInternal(@MappingTarget CommonSettings.PlannerSettings defaultPlannerSettings,
                                                        DistributedTaskProperties.Planner planner) {
        DistributedTaskProperties.Planner defaultPropertiesPlanner = map(defaultPlannerSettings);
        DistributedTaskProperties.Planner mergedPlanner = merge(defaultPropertiesPlanner, planner);
        return map(mergedPlanner).toBuilder()
            .pollingDelay(defaultPlannerSettings.getPollingDelay()) //save as is here
            .build();
    }

    @Mapping(target = "pollingDelay", ignore = true)
    public abstract DistributedTaskProperties.Planner map(CommonSettings.PlannerSettings defaultPlannerSettings);

    @Mapping(target = "pollingDelay", ignore = true)
    public abstract DistributedTaskProperties.Planner merge(@MappingTarget DistributedTaskProperties.Planner defaultPropertiesPlanner,
                                                            DistributedTaskProperties.Planner planner);

    @Mapping(target = "pollingDelay", ignore = true)
    public abstract CommonSettings.PlannerSettings map(DistributedTaskProperties.Planner mergedPlanner);
}
