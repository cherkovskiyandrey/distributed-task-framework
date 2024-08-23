package com.distributed_task_framework.saga.configurations;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@Data
@Builder(toBuilder = true)
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@NoArgsConstructor
@ConfigurationProperties(prefix = "distributed-task.saga")
public class SagaConfiguration {
    @Builder.Default
    Result result = Result.builder().build();

    @Validated
    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Result {
        @Builder.Default
        Duration resultScanInitialDelay = Duration.ofSeconds(10);
        @Builder.Default
        Duration resultScanFixedDelay = Duration.ofSeconds(10);
        @Builder.Default
        Duration emptyResultDeprecationTimeout = Duration.ofMinutes(1);
        @Builder.Default
        Duration resultDeprecationTimeout = Duration.ofMinutes(1);
    }
}
