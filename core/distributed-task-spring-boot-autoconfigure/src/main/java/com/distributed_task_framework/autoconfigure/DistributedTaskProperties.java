package com.distributed_task_framework.autoconfigure;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.task.Task;
import com.google.common.collect.Maps;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

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
        /**
         * Current application name.
         */
        @NotBlank
        String appName;
        Completion completion;
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
    public static class Completion {
        Duration handlerInitialDelay;
        Duration handlerFixedDelay;
        Duration defaultTaskTimeout;
        Duration defaultWorkflowTimeout;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Registry {
        /**
         * Initial delay before start to update node state in ms.
         */
        Integer updateInitialDelayMs;
        /**
         * Delay between updates of node state in ms.
         */
        Integer updateFixedDelayMs;
        /**
         * Max interval to unregister node when node doesn't update status.
         */
        Integer maxInactivityIntervalMs;
        /**
         * Cache expiration in sec to for registered cluster information.
         */
        Integer cacheExpirationMs;
        /**
         * Duration of time cpu loading of current node is calculated in.
         */
        Duration cpuCalculatingTimeWindow;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Planner {
        /**
         * Initial delay before start to watch by current active planner in ms.
         */
        Integer watchdogInitialDelayMs;
        /**
         * Delay between watching by current active planner in ms.
         */
        Integer watchdogFixedDelayMs;
        /**
         * How many parallel tasks can be in the cluster for unknown task on planner.
         * -1 means unlimited.
         */
        Integer maxParallelTasksInClusterDefault;
        /**
         * Max number tasks to plan.
         */
        Integer batchSize;
        /**
         * Batch size to move tasks from NEW virtual queue.
         */
        Integer newBatchSize;
        /**
         * Batch size to handle tasks from DELETED virtual queue.
         */
        Integer deletedBatchSize;
        /**
         * Time overlap is used in order to scan affinity groups from NEW virtual queue.
         */
        Duration affinityGroupScannerTimeOverlap;
        /**
         * Time window to track pairs of affinityGroup and taskName in ACTIVE virtual queue.
         */
        Duration partitionTrackingTimeWindow;
        /**
         * The limit of cpu loading for nodes.
         * If node reach this limit, planner will not consider this node to plan.
         * Range from 0.0-1.0.
         */
        Double nodeCpuLoadingLimit;
        /**
         * Used to plan bigger than capacity in this factor times.
         * In order to reduce potential delay between planner loop steps.
         * Take into account that work only for unlimited tasks
         * (maxParallelTasksInClusterDefault = UNLIMITED_PARALLEL_TASKS and TaskSettings.maxParallelInCluster = UNLIMITED_PARALLEL_TASKS)
         */
        Float planFactor;
        /**
         * Function describe delay between polling of db depends on last number of ready to plan tasks.
         * Key is a number of tasks in last polling.
         * Value is a delay in ms before next polling.
         */
        @Builder.Default
        Map<Integer, Integer> pollingDelay = Maps.newHashMap();
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class WorkerManager {
        /**
         * How many parallel tasks can be run on one node.
         */
        Integer maxParallelTasksInNode;
        /**
         * Function describe delay between manage of tasks depend on input tasks number.
         * Key is a number of tasks in last polling.
         * Value is a delay in ms before next polling.
         */
        @Builder.Default
        Map<Integer, Integer> manageDelay = Maps.newHashMap();
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Statistics {
        /**
         * Initial delay before start to calculate task statistic in ms.
         */
        Integer calcInitialDelayMs;
        /**
         * Delay between calculation of task statistics in ms.
         */
        Integer calcFixedDelayMs;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class DeliveryManager {
        RemoteApps remoteApps;
        /**
         * Initial delay before start to watch by current active planner in ms.
         */
        Integer watchdogInitialDelayMs;
        /**
         * Delay between watching by current active planner in ms.
         */
        Integer watchdogFixedDelayMs;
        /**
         * How many commands can be sent in one request.
         */
        Integer batchSize;
        /**
         * Timeout to connect to remote app.
         */
        Duration connectionTimeout;
        /**
         * Specifies the maximum duration allowed between each network-level
         * read operation while reading a given response from remote app.
         */
        Duration responseTimeout;
        /**
         * The connection is closed when there is no inbound traffic during this time from remote app.
         */
        Duration readTimeout;
        /**
         * The connection is closed when a write operation cannot finish in this time.
         */
        Duration writeTimeout;
        /**
         * Retry policy for commands to send.
         * After all attempts are failed, commands will be moved to DLC.
         */
        Retry retry;
        /**
         * Function describe delay between polling remote commands depend on input command number.
         * Key is a number of command in last polling.
         * Value is a delay in ms before next polling.
         */
        @Builder.Default
        Map<Integer, Integer> manageDelay = Maps.newHashMap();
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class RemoteApps {
        /**
         * Mapping of name remote application to its URL.
         * Used in order to execute remote (outside of current cluster) tasks.
         */
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
        /**
         * Execution guarantees.
         */
        String executionGuarantees; // @TaskExecutionGuarantees
        /**
         * Has this task to be moved to dlt when eventually failed.
         */
        Boolean dltEnabled; //@TaskDltEnable
        /**
         * Cron string or time duration string {@link Duration#parse(CharSequence)}
         */
        String cron; // @TaskSchedule
        /**
         * Retry policy for task.
         */
        Retry retry; //@TaskFixedRetryPolicy
        /**
         * How many parallel tasks can be in the cluster.
         * '-1' means undefined and depends on current cluster configuration.
         */
        Integer maxParallelInCluster; // @TaskConcurrency
        /**
         * How many parallel tasks can be on the one node (one worker).
         * '-1' means undefined.
         */
        Integer maxParallelInNode; // @TaskConcurrency
        /**
         * Task timeout. If task still is in progress after timeout expired, it will be interrupted.
         * {@link InterruptedException} will be risen in {@link Task#execute(ExecutionContext)}
         */
        Duration timeout; // @TaskTimeout
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Retry {
        /**
         * Sets default retry mode.
         */
        String retryMode;
        Fixed fixed;
        Backoff backoff;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Fixed {
        /**
         * Delay between retires.
         */
        Duration delay;
        /**
         * Max attempts.
         */
        Integer maxNumber;
        /**
         * Max interval for retires.
         * Give up after whether max attempts is reached or interval is passed.
         */
        Duration maxInterval;
    }

    @Data
    @Builder(toBuilder = true)
    @AllArgsConstructor(access = AccessLevel.PUBLIC)
    @NoArgsConstructor
    public static class Backoff {
        /**
         * Initial delay of the first retry.
         */
        Duration initialDelay;
        /**
         * The time interval that is the ratio of the exponential backoff formula (geometric progression).
         */
        Duration delayPeriod;
        /**
         * Maximum number of times a tuple is retried before being acked and scheduled for commit.
         */
        Integer maxRetries;
        /**
         * Maximum amount of time waiting before retrying.
         */
        Duration maxDelay;
    }
}
