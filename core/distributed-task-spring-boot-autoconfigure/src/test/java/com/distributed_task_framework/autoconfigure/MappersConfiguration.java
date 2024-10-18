package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.autoconfigure.mapper.CommonSettingsMerger;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@Configuration
@ComponentScan(basePackageClasses = CommonSettingsMerger.class)
public class MappersConfiguration {
}
