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
public abstract class WorkerManagerSettingsMerger {
    @Autowired
    private RangeDelayMapper rangeDelayMapper;

    public CommonSettings.WorkerManagerSettings merge(@MappingTarget CommonSettings.WorkerManagerSettings workerManagerSettings,
                                                      @Nullable DistributedTaskProperties.WorkerManager workerManager) {
        if (workerManager == null) {
            return workerManagerSettings;
        }
        
        var mergedManagerSettings = mergeInternal(workerManagerSettings, workerManager);
        if (!workerManager.getManageDelay().isEmpty()) {
            ImmutableRangeMap<Integer, Integer> manageDelay = rangeDelayMapper.mapRangeDelayProperty(workerManager.getManageDelay());
            mergedManagerSettings = mergedManagerSettings.toBuilder()
                .manageDelay(manageDelay)
                .build();
        }
        return mergedManagerSettings;
    }

    public CommonSettings.WorkerManagerSettings mergeInternal(@MappingTarget CommonSettings.WorkerManagerSettings workerManagerSettings,
                                                              DistributedTaskProperties.WorkerManager workerManager) {
        DistributedTaskProperties.WorkerManager defaultWorkerManager = map(workerManagerSettings);
        DistributedTaskProperties.WorkerManager mergedRegistry = merge(defaultWorkerManager, workerManager);
        return map(mergedRegistry).toBuilder()
            .manageDelay(workerManagerSettings.getManageDelay()) //save as is here
            .build();
    }

    @Mapping(target = "manageDelay", ignore = true)
    public abstract DistributedTaskProperties.WorkerManager map(CommonSettings.WorkerManagerSettings workerManagerSettings);

    @Mapping(target = "manageDelay", ignore = true)
    public abstract DistributedTaskProperties.WorkerManager merge(@MappingTarget DistributedTaskProperties.WorkerManager defaultWorkerManager,
                                                                  DistributedTaskProperties.WorkerManager workerManager);

    @Mapping(target = "manageDelay", ignore = true)
    public abstract CommonSettings.WorkerManagerSettings map(DistributedTaskProperties.WorkerManager mergedRegistry);
}
