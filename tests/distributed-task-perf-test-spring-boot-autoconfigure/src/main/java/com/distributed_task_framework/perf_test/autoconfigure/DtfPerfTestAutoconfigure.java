package com.distributed_task_framework.perf_test.autoconfigure;

import com.distributed_task_framework.service.DistributedTaskService;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import com.distributed_task_framework.autoconfigure.DistributedTaskAutoconfigure;
import com.distributed_task_framework.perf_test.persistence.repository.StressTestSummaryRepository;

@Configuration
@ConditionalOnClass(DistributedTaskService.class)
@ConditionalOnProperty(name = "distributed-task.enabled", havingValue = "true")
@AutoConfigureAfter({
        DistributedTaskAutoconfigure.class,
})
@EnableJdbcRepositories(basePackageClasses = StressTestSummaryRepository.class)
public class DtfPerfTestAutoconfigure {

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        var objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        //in order to easily add new properties to the task message and be tolerant during rolling out
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
        objectMapper.configure(SerializationFeature.WRITE_SELF_REFERENCES_AS_NULL, true);
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper;
    }
}
