package com.distributed_task_framework.saga.autoconfigure.utils;

import com.distributed_task_framework.saga.autoconfigure.mappers.SagaCommonPropertiesMapper;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@Configuration
@ComponentScan(basePackageClasses = SagaCommonPropertiesMapper.class)
public class MappersConfiguration {
}
