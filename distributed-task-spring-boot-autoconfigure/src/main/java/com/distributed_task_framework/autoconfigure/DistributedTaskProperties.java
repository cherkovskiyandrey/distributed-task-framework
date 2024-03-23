package com.distributed_task_framework.autoconfigure;

import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.net.URL;
import java.time.Duration;
import java.util.Map;

@Validated
@Data
@ConfigurationProperties(prefix = "distributed-task")
public class DistributedTaskProperties {
    @NotNull
    Common common;
    TaskPropertiesGroup taskPropertiesGroup;

    @Validated
    @Data
    @Builder
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Common {
        @NotBlank
        String appName;
        Registry registry;
        Planner planner;
        WorkerManager workerManager;
        Statistics statistics;
        DeliveryManager deliveryManager;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Registry {
        Integer updateInitialDelayMs;
        Integer updateFixedDelayMs;
        Integer maxInactivityIntervalMs;
        Integer cacheExpirationMs;
        Duration cpuCalculatingTimeWindow;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Planner {
        Integer watchdogInitialDelayMs;
        Integer watchdogFixedDelayMs;
        Integer maxParallelTasksInClusterDefault;
        Integer batchSize;
        Integer newBatchSize;
        Integer deletedBatchSize;
        Duration affinityGroupScannerTimeOverlap;
        Duration partitionTrackingTimeWindow;
        Double nodeCpuLoadingLimit;
        Float planFactor;
        @Builder.Default
        Map<Integer, Integer> pollingDelay = Maps.newHashMap();
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class WorkerManager {
        Integer maxParallelTasksInNode;
        @Builder.Default
        Map<Integer, Integer> manageDelay = Maps.newHashMap();
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Statistics {
        Integer calcInitialDelayMs;
        Integer calcFixedDelayMs;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class DeliveryManager {
        RemoteApps remoteApps;
        Integer watchdogInitialDelayMs;
        Integer watchdogFixedDelayMs;
        Integer batchSize;
        Duration connectionTimeout;
        Duration responceTimeout;
        Duration readTimeout;
        Duration writeTimeout;
        Retry retry;
        @Builder.Default
        Map<Integer, Integer> manageDelay = Maps.newHashMap();
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class RemoteApps {
        @Builder.Default
        Map<String, URL> appToUrl = Maps.newHashMap();
    }

    @Data
    @Builder
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class TaskPropertiesGroup {
        TaskProperties defaultProperties;
        @Builder.Default
        Map<String, TaskProperties> taskProperties = Map.of();
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class TaskProperties {
        String executionGuarantees; // @TaskExecutionGuarantees
        Boolean dltEnabled; //@TaskDltEnable
        String cron; // @TaskSchedule
        Retry retry; //@TaskFixedRetryPolicy
        Integer maxParallelInCluster; // @TaskConcurrency
        Duration timeout; // @TaskTimeout
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Retry {
        String retryMode;
        Fixed fixed;
        Backoff backoff;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Fixed {
        Duration delay;
        Integer maxNumber;
        Duration maxInterval;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Backoff {
        Duration initialDelay;
        Duration delayPeriod;
        Integer maxRetries;
        Duration maxDelay;
    }
}
