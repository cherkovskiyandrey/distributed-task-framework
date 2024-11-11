package com.distributed_task_framework.autoconfigure.mapper;


import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.autoconfigure.MappersConfiguration;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.RetryMode;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.net.URL;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBootTest
@ContextConfiguration(classes = MappersConfiguration.class)
@FieldDefaults(level = AccessLevel.PRIVATE)
class CommonSettingsMergerTest {
    @Autowired
    CommonSettingsMerger commonSettingsMerger;

    @SneakyThrows
    @Test
    void shouldMergeCommonPropertiesWhenDefault() {
        //when & do
        CommonSettings overrideCommonSettings = commonSettingsMerger.merge(
            CommonSettings.DEFAULT,
            DistributedTaskProperties.Common.builder()
                .appName("test")
                .deliveryManager(DistributedTaskProperties.DeliveryManager.builder()
                    .remoteApps(
                        DistributedTaskProperties.RemoteApps.builder()
                            .appToUrl(Map.of(
                                "remote-app", new URL("http://remote-app")
                            ))
                            .build()
                    )
                    .build()
                )
                .build()
        );

        //verify
        CommonSettings expectedProperties = CommonSettings.DEFAULT.toBuilder()
            .appName("test")
            .deliveryManagerSettings(CommonSettings.DEFAULT.getDeliveryManagerSettings().toBuilder()
                .remoteApps(CommonSettings.RemoteApps.builder()
                    .appToUrl(Map.of(
                        "remote-app", new URL("http://remote-app")
                    ))
                    .build()
                )
                .build()
            )
            .build();
        assertThat(overrideCommonSettings).isEqualTo(expectedProperties);
    }

    @SneakyThrows
    @Test
    void shouldMergeCommonPropertiesWhenOverride() {
        //when & do
        CommonSettings overrideCommonSettings = commonSettingsMerger.merge(
            CommonSettings.DEFAULT.toBuilder().build(),
            DistributedTaskProperties.Common.builder()
                .appName("test")
                .completion(DistributedTaskProperties.Completion.builder()
                    .handlerFixedDelay(Duration.ofMinutes(10))
                    .defaultWorkflowTimeout(Duration.ofMinutes(100))
                    .build()
                )
                .registry(DistributedTaskProperties.Registry.builder()
                    .updateFixedDelayMs(6000)
                    .cacheExpirationMs(3000)
                    .build()
                )
                .planner(DistributedTaskProperties.Planner.builder()
                    .watchdogInitialDelayMs(6000)
                    .batchSize(100)
                    .pollingDelay(Map.of(
                        0, 10_000,
                        100, 5_000,
                        1000, 1_000
                    ))
                    .build()
                )
                .workerManager(DistributedTaskProperties.WorkerManager.builder()
                    .maxParallelTasksInNode(50)
                    .manageDelay(Map.of(
                        0, 10_000,
                        100, 5_000,
                        1000, 1_000
                    ))
                    .build()
                )
                .statistics(DistributedTaskProperties.Statistics.builder()
                    .calcFixedDelayMs(9)
                    .build()
                )
                .deliveryManager(DistributedTaskProperties.DeliveryManager.builder()
                    .remoteApps(
                        DistributedTaskProperties.RemoteApps.builder()
                            .appToUrl(Map.of(
                                "remote-app", new URL("http://remote-app")
                            ))
                            .build()
                    )
                    .batchSize(200)
                    .retry(DistributedTaskProperties.Retry.builder()
                        .retryMode(RetryMode.FIXED.toString())
                        .fixed(DistributedTaskProperties.Fixed.builder()
                            .maxNumber(10)
                            .build())
                        .build())
                    .manageDelay(Map.of(
                            0, 10_000,
                            100, 5_000,
                            1000, 1_000
                        )
                    )
                    .build()
                )
                .build()
        );

        //verify
        CommonSettings expectedProperties = CommonSettings.DEFAULT.toBuilder()
            .appName("test")
            .completionSettings(CommonSettings.DEFAULT.getCompletionSettings().toBuilder()
                .handlerFixedDelay(Duration.ofMinutes(10))
                .defaultWorkflowTimeout(Duration.ofMinutes(100))
                .build()
            )
            .registrySettings(CommonSettings.DEFAULT.getRegistrySettings().toBuilder()
                .updateFixedDelayMs(6000)
                .cacheExpirationMs(3000)
                .build()
            )
            .plannerSettings(CommonSettings.DEFAULT.getPlannerSettings().toBuilder()
                .watchdogInitialDelayMs(6000)
                .batchSize(100)
                .pollingDelay(ImmutableRangeMap.<Integer, Integer>builder()
                    .put(Range.openClosed(-1, 0), 10_000)
                    .put(Range.openClosed(0, 100), 5_000)
                    .put(Range.openClosed(100, 1000), 1_000)
                    .build()
                )
                .build()
            )
            .workerManagerSettings(CommonSettings.DEFAULT.getWorkerManagerSettings().toBuilder()
                .maxParallelTasksInNode(50)
                .manageDelay(ImmutableRangeMap.<Integer, Integer>builder()
                    .put(Range.openClosed(-1, 0), 10_000)
                    .put(Range.openClosed(0, 100), 5_000)
                    .put(Range.openClosed(100, 1000), 1_000)
                    .build()
                )
                .build()
            )
            .statSettings(CommonSettings.DEFAULT.getStatSettings().toBuilder()
                .calcFixedDelayMs(9)
                .build()
            )
            .deliveryManagerSettings(CommonSettings.DEFAULT.getDeliveryManagerSettings().toBuilder()
                .remoteApps(CommonSettings.RemoteApps.builder()
                    .appToUrl(Map.of(
                        "remote-app", new URL("http://remote-app")
                    ))
                    .build())
                .batchSize(200)
                .retry(CommonSettings.DEFAULT.getDeliveryManagerSettings().getRetry().toBuilder()
                    .retryMode(RetryMode.FIXED)
                    .fixed(CommonSettings.DEFAULT.getDeliveryManagerSettings().getRetry().getFixed().toBuilder()
                        .maxNumber(10)
                        .build()
                    )
                    .build()
                )
                .manageDelay(ImmutableRangeMap.<Integer, Integer>builder()
                    .put(Range.openClosed(-1, 0), 10_000)
                    .put(Range.openClosed(0, 100), 5_000)
                    .put(Range.openClosed(100, 1000), 1_000)
                    .build())
                .build()
            )
            .build();
        assertThat(overrideCommonSettings).isEqualTo(expectedProperties);
    }
}
