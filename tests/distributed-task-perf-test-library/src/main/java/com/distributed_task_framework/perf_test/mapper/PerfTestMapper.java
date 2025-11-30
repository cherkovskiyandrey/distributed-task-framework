package com.distributed_task_framework.perf_test.mapper;

import com.distributed_task_framework.perf_test.model.PerfTestRunResult;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestRun;
import com.distributed_task_framework.perf_test.persistence.entity.PerfTestState;
import com.distributed_task_framework.perf_test.tasks.dto.PerfTestGeneratedSpecDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.NullValueCheckStrategy;
import org.mapstruct.NullValuePropertyMappingStrategy;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE,
        nullValueCheckStrategy = NullValueCheckStrategy.ALWAYS,
        imports = {LocalDateTime.class, Duration.class}
)
public interface PerfTestMapper {

    @Mapping(target = "createdAt", expression = "java(LocalDateTime.now())")
    @Mapping(target = "id", ignore = true)
    PerfTestRun toRun(PerfTestGeneratedSpecDto specDto);

    @Mapping(target = "summaryStates", source = "summaryStates")
    @Mapping(target = "duration", expression = "java(specDto.getCreatedAt() != null && completedAt != null ? Duration.between(specDto.getCreatedAt(), completedAt) : null)")
    PerfTestRunResult toTestResult(PerfTestRun specDto,
                                   LocalDateTime completedAt,
                                   Map<PerfTestState, Long> summaryStates);
}
