package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.settings.TaskSettings;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(
    unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface SettingsMapper {
    TaskSettings map(SagaMethodSettings settings);
}
