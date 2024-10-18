package com.distributed_task_framework.autoconfigure.mapper;

import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.settings.CommonSettings;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;
import org.springframework.beans.factory.annotation.Autowired;


@Mapper(
    componentModel = MappingConstants.ComponentModel.SPRING,
    uses = {
        RangeDelayMapper.class,
        RetrySettingsMerger.class
    },
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public abstract class DeliveryManagerSettingsMerger {
    @Autowired
    private RangeDelayMapper rangeDelayMapper;

    public CommonSettings.DeliveryManagerSettings merge(@MappingTarget CommonSettings.DeliveryManagerSettings deliveryManagerSettings,
                                                        @Nullable DistributedTaskProperties.DeliveryManager deliveryManager) {
        if (deliveryManager == null) {
            return deliveryManagerSettings;
        }

        var mergedManagerSettings = mergeInternal(deliveryManagerSettings, deliveryManager);
        if (!deliveryManager.getManageDelay().isEmpty()) {
            var polingDelay = rangeDelayMapper.mapRangeDelayProperty(deliveryManager.getManageDelay());
            mergedManagerSettings = mergedManagerSettings.toBuilder()
                .manageDelay(polingDelay)
                .build();
        }
        return mergedManagerSettings;
    }

    public CommonSettings.DeliveryManagerSettings mergeInternal(@MappingTarget CommonSettings.DeliveryManagerSettings deliveryManagerSettings,
                                                                DistributedTaskProperties.DeliveryManager deliveryManager) {
        DistributedTaskProperties.DeliveryManager defaultDeliveryManager = map(deliveryManagerSettings);
        DistributedTaskProperties.DeliveryManager mergedRegistry = merge(defaultDeliveryManager, deliveryManager);
        return map(mergedRegistry).toBuilder()
            .manageDelay(deliveryManagerSettings.getManageDelay()) //save as is here
            .build();
    }

    @Mapping(target = "manageDelay", ignore = true)
    public abstract DistributedTaskProperties.DeliveryManager map(CommonSettings.DeliveryManagerSettings deliveryManagerSettings);

    @Mapping(target = "manageDelay", ignore = true)
    public abstract DistributedTaskProperties.DeliveryManager merge(@MappingTarget DistributedTaskProperties.DeliveryManager deliveryManagerSettings,
                                                                    DistributedTaskProperties.DeliveryManager workerManager);

    @Mapping(target = "manageDelay", ignore = true)
    public abstract CommonSettings.DeliveryManagerSettings map(DistributedTaskProperties.DeliveryManager mergedRegistry);
}
