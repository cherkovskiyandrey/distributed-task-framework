package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.settings.TaskSettings;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(
    unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface SettingsMapper {

    @Mapping(target = "dltEnabled", constant = "false") //dlt doesn't make sense for saga, because saga is interactive
    @Mapping(target = "cron", expression = "java(null)") //cron doesn't make sense for saga
    TaskSettings map(SagaMethodSettings settings);
}
