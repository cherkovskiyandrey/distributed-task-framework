package com.distributed_task_framework.settings;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.With;

import java.net.URL;
import java.time.Duration;
import java.util.Map;

@Value
@Builder(toBuilder = true)
public class CommonSettings {
    public static final Retry DEFAULT_RETRY = cerateDefaultRetry();

    private static Retry cerateDefaultRetry() {
        return Retry.builder()
                .retryMode(RetryMode.FIXED)
                .fixed(Fixed.builder().build())
                .backoff(Backoff.builder().build())
                .build();
    }

    public static final CommonSettings DEFAULT = CommonSettings.builder().build();

    /**
     * Current application name.
     */
    String appName;

    @Builder.Default
    CompletionSettings completionSettings = createCompletionSettings();

    private static CompletionSettings createCompletionSettings() {
        return CompletionSettings.builder()
                .handlerInitialDelay(Duration.ofSeconds(1))
                .handlerFixedDelay(Duration.ofSeconds(1))
                .defaultTaskTimeout(Duration.ofMinutes(1))
                .defaultWorkflowTimeout(Duration.ofMinutes(1))
                .build();
    }

    @Builder.Default
    RegistrySettings registrySettings = createDefaultRegistrySettings();

    private static RegistrySettings createDefaultRegistrySettings() {
        return RegistrySettings.builder()
            .updateInitialDelayMs(0)
            .updateFixedDelayMs(4000)
            .maxInactivityIntervalMs(20000)
            .cacheExpirationMs(1000)
            //one metric in 4 sec for 5 minutes = (60/4)*5 = 15*5 = 50+25 = 75 values
            .cpuCalculatingTimeWindow(Duration.ofMinutes(5))
            .build();
    }

    @Builder.Default
    PlannerSettings plannerSettings = createDefaultPlannerSettings();

    private static PlannerSettings createDefaultPlannerSettings() {
        return PlannerSettings.builder()
            .watchdogInitialDelayMs(5000)
            .watchdogFixedDelayMs(5000)
            .maxParallelTasksInClusterDefault(PlannerSettings.UNLIMITED_PARALLEL_TASKS)
            .fetchFactor(2.F) //current tasks + the same with same affinity group and affinity
            .planFactor(2.F) //twice bigger than free capacity
            .batchSize(1000)
            .newBatchSize(300)
            .deletedBatchSize(300)
            .pollingDelay(ImmutableRangeMap.<Integer, Integer>builder()
                .put(Range.openClosed(-1, 0), 1000)
                .put(Range.openClosed(0, 100), 500)
                .put(Range.openClosed(100, 1000), 0)
                .build()
            )
            .affinityGroupScannerTimeOverlap(Duration.ofMinutes(1))
            .partitionTrackingTimeWindow(Duration.ofMinutes(1))
            .nodeCpuLoadingLimit(0.95)
            .build();
    }

    @Builder.Default
    WorkerManagerSettings workerManagerSettings = createDefaultWorkerManagerSettings();

    private static WorkerManagerSettings createDefaultWorkerManagerSettings() {
        return WorkerManagerSettings.builder()
            .maxParallelTasksInNode(100)
            .manageDelay(ImmutableRangeMap.<Integer, Integer>builder()
                .put(Range.openClosed(-1, 0), 1000)
                .put(Range.openClosed(0, 50), 500)
                .put(Range.openClosed(50, 100), 250)
                .build()
            )
            .build();
    }

    @Builder.Default
    StatSettings statSettings = createDefaultStatSettings();

    private static StatSettings createDefaultStatSettings() {
        return StatSettings.builder()
            .calcInitialDelayMs(10_000)
            .calcFixedDelayMs(10_000)
            .build();
    }

    @Builder.Default
    DeliveryManagerSettings deliveryManagerSettings = createDefaultDeliveryManagerSettings();

    private static DeliveryManagerSettings createDefaultDeliveryManagerSettings() {
        return DeliveryManagerSettings.builder()
            .watchdogInitialDelayMs(5000)
            .watchdogFixedDelayMs(5000)
            .batchSize(100)
            .connectionTimeout(Duration.ofSeconds(5))
            .responseTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(5))
            .writeTimeout(Duration.ofSeconds(30))
            .manageDelay(ImmutableRangeMap.<Integer, Integer>builder()
                .put(Range.openClosed(-1, 0), 1000)
                .put(Range.openClosed(0, 100), 500)
                .put(Range.openClosed(100, 1000), 250)
                .build())
            .build();
    }

    @Value
    @Builder(toBuilder = true)
    public static class PlannerSettings {
        public static final int UNLIMITED_PARALLEL_TASKS = -1;
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
         * -1 means unlimited
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
         * The limit of cpu loading for nodes.
         * If node reach this limit, planner will not consider this node to plan.
         * Range from 0.0-1.0
         */
        Double nodeCpuLoadingLimit;
        /**
         * Used to calculate batch size base on current free capacity.
         * Usually allow to take into account dependent tasks.
         */
        Float fetchFactor;
        /**
         * Used to plan bigger than capacity in this factor times.
         * In order to reduce potential delay between planner loop steps.
         */
        Float planFactor;
        /**
         * Time window to track pairs of affinityGroup and taskName in ACTIVE virtual queue
         */
        Duration partitionTrackingTimeWindow;
        /**
         * Function describe delay between polling of db depends on last number of ready to plan tasks.
         * key - number of tasks in last polling
         * value - delay in ms before next polling
         */
        @Builder.Default
        ImmutableRangeMap<Integer, Integer> pollingDelay = ImmutableRangeMap.<Integer, Integer>builder().build();
    }

    @Value
    @Builder(toBuilder = true)
    public static class RegistrySettings {
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

    @Value
    @Builder(toBuilder = true)
    public static class WorkerManagerSettings {
        /**
         * How many parallel tasks can be run on one node.
         */
        Integer maxParallelTasksInNode;
        /**
         * Function describe delay between manage of tasks depend on input tasks number.
         * key - number of tasks in last polling
         * value - delay in ms before next polling
         */
        @Builder.Default
        ImmutableRangeMap<Integer, Integer> manageDelay = ImmutableRangeMap.<Integer, Integer>builder().build();
    }

    @Value
    @Builder(toBuilder = true)
    public static class StatSettings {
        /**
         * Initial delay before start to calculate task statistic in ms.
         */
        Integer calcInitialDelayMs;
        /**
         * Delay between calculation of task statistics in ms.
         */
        Integer calcFixedDelayMs;
    }

    @Value
    @Builder(toBuilder = true)
    public static class RemoteApps {
        /**
         * Mapping of name remote application to its URL.
         * Used in order to execute remote (outside of current cluster) tasks.
         */
        @Builder.Default
        Map<String, URL> appToUrl = Maps.newHashMap();
    }

    @Value
    @Builder(toBuilder = true)
    public static class DeliveryManagerSettings {

        @Builder.Default
        RemoteApps remoteApps = RemoteApps.builder()
            .build();

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
         * After all attempts are failed, commands will be moved to DLC
         */
        @Builder.Default
        Retry retry = DEFAULT_RETRY.toBuilder().build();

        /**
         * Function describe delay between polling remote commands depend on input command number.
         * key - number of command in last polling
         * value - delay in ms before next polling
         */
        @Builder.Default
        ImmutableRangeMap<Integer, Integer> manageDelay = ImmutableRangeMap.<Integer, Integer>builder().build();
    }

    @Value
    @Builder(toBuilder = true)
    public static class CompletionSettings {
        /**
         * Initial delay before start to handle status of completion tasks.
         */
        Duration handlerInitialDelay;
        /**
         * Delay between handling status of completion tasks.
         */
        Duration handlerFixedDelay;
        /**
         * Default timeout to wait for completion particular task operation.
         */
        Duration defaultTaskTimeout;

        /**
         * Default timeout to wait for completion all workflow.
         */
        Duration defaultWorkflowTimeout;
    }
}
