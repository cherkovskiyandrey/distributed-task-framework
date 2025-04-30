package com.distributed_task_framework.saga.autoconfigure.mappers;

import com.distributed_task_framework.autoconfigure.mapper.RetrySettingsMerger;
import com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties.SagaMethodProperties;
import com.google.common.collect.Sets;
import jakarta.annotation.Nullable;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.MappingTarget;
import org.mapstruct.NullValuePropertyMappingStrategy;

import java.util.List;

@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        uses = RetrySettingsMerger.class,
        nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface SagaMethodPropertiesMerger {

    SagaMethodProperties merge(@MappingTarget SagaMethodProperties defaultSagaMethodProperties,
                               @Nullable SagaMethodProperties sagaMethodProperties);

    @SuppressWarnings("UnusedReturnValue")
    default List<Class<? extends Throwable>> merge(@MappingTarget List<Class<? extends Throwable>> defaultList,
                                                   @Nullable List<Class<? extends Throwable>> list) {
        if (list != null) {
            defaultList.addAll(list);
            var unique = Sets.newHashSet(defaultList);
            defaultList.clear();
            defaultList.addAll(unique);
        }
        return defaultList;
    }
}
