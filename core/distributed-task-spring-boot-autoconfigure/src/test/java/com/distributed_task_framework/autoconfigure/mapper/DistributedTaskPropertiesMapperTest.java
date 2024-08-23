package com.distributed_task_framework.autoconfigure.mapper;


import com.distributed_task_framework.autoconfigure.DistributedTaskProperties;
import com.distributed_task_framework.settings.CommonSettings;
import com.distributed_task_framework.settings.RetryMode;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URL;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@FieldDefaults(level = AccessLevel.PRIVATE)
@ExtendWith(MockitoExtension.class)
class DistributedTaskPropertiesMapperTest {
    private final DistributedTaskPropertiesMapper distributedTaskPropertiesMapper = Mappers.getMapper(DistributedTaskPropertiesMapper.class);

    //todo
    @SuppressWarnings({"deprecation", "UnstableApiUsage"})
    @SneakyThrows
    @Test
    void shouldMergeCommonPropertiesWhenDefault() {
        //when & do
        CommonSettings overrideCommonSettings = distributedTaskPropertiesMapper.merge(
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

    @SuppressWarnings({"deprecation", "UnstableApiUsage"})
    @SneakyThrows
    @Test
    void shouldMergeCommonPropertiesWhenOverride() {
        //when & do
        CommonSettings overrideCommonSettings = distributedTaskPropertiesMapper.merge(
                CommonSettings.builder().build(),
                DistributedTaskProperties.Common.builder()
                        .appName("test")
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
                                .build())
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
                                .build())
                        .build()
                )
                .workerManagerSettings(CommonSettings.DEFAULT.getWorkerManagerSettings().toBuilder()
                        .maxParallelTasksInNode(50)
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
                                        .build())
                                .build())
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
