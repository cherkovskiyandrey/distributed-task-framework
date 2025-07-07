package com.distributed_task_framework.saga.autoconfigure.services.impl;

import com.distributed_task_framework.saga.autoconfigure.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties;
import com.distributed_task_framework.saga.services.SagaRegisterSettingsService;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SagaPropertiesProcessorImplTest extends BaseSpringIntegrationTest {

    @Nested
    class BuildSagaCommonSettingsTest {

        @Test
        void shouldUseDefaultWhenBuildSagaCommonSettings() {
            // when & do
            var settings = sagaPropertiesProcessor.buildSagaCommonSettings(null);

            // verify
            assertThat(settings).isEqualTo(SagaCommonSettings.buildDefault());
        }

        @Test
        void shouldUseDefaultAndPartiallyOverrideWhenBuildSagaCommonSettings() {
            // when
            var commonProperties = DistributedSagaProperties.Common.builder()
                .cacheExpiration(Duration.ofSeconds(1))
                .deprecatedSagaScanInitialDelay(Duration.ofSeconds(2))
                .build();

            // do
            var settings = sagaPropertiesProcessor.buildSagaCommonSettings(commonProperties);

            // verify
            assertThat(settings).isEqualTo(SagaCommonSettings.buildDefault().toBuilder()
                .cacheExpiration(Duration.ofSeconds(1))
                .deprecatedSagaScanInitialDelay(Duration.ofSeconds(2))
                .build()
            );
        }

        @Test
        void shouldOverrideAllWhenBuildSagaCommonSettings() {
            // when
            var commonProperties = DistributedSagaProperties.Common.builder()
                .cacheExpiration(Duration.ofSeconds(1))
                .deprecatedSagaScanInitialDelay(Duration.ofSeconds(2))
                .deprecatedSagaScanFixedDelay(Duration.ofSeconds(3))
                .build();

            // do
            var settings = sagaPropertiesProcessor.buildSagaCommonSettings(commonProperties);

            // verify
            assertThat(settings).isEqualTo(SagaCommonSettings.builder()
                .cacheExpiration(Duration.ofSeconds(1))
                .deprecatedSagaScanInitialDelay(Duration.ofSeconds(2))
                .deprecatedSagaScanFixedDelay(Duration.ofSeconds(3))
                .build()
            );
        }
    }

    @Nested
    class RegisterConfiguredSagasTest {
        @Test
        void shouldUseDefaultWhenRegisterConfiguredSagas() {
            // when
            var sagaRegisterSettingsService = Mockito.mock(SagaRegisterSettingsService.class);

            // do
            sagaPropertiesProcessor.registerConfiguredSagas(sagaRegisterSettingsService, null);

            // verify
            Mockito.verify(sagaRegisterSettingsService).registerDefaultSagaSettings(
                Mockito.eq(SagaSettings.buildDefault())
            );
        }

        @Test
        void shouldUseDefaultAndPartiallyOverrideWhenRegisterConfiguredSagas() {
            // when
            var sagaRegisterSettingsService = Mockito.mock(SagaRegisterSettingsService.class);
            var sagaPropertiesGroup = DistributedSagaProperties.SagaPropertiesGroup.builder()
                .sagaPropertiesGroup(Map.of(
                        "test",
                        DistributedSagaProperties.SagaProperties.builder()
                            .availableAfterCompletionTimeout(Duration.ofSeconds(10))
                            .expirationTimeout(Duration.ofSeconds(20))
                            .build()
                    )
                )
                .build();

            // do
            sagaPropertiesProcessor.registerConfiguredSagas(sagaRegisterSettingsService, sagaPropertiesGroup);

            // verify
            InOrder inOrder = Mockito.inOrder(sagaRegisterSettingsService);
            inOrder.verify(sagaRegisterSettingsService).registerDefaultSagaSettings(
                Mockito.eq(SagaSettings.buildDefault())
            );
            inOrder.verify(sagaRegisterSettingsService).registerSagaSettings(
                Mockito.eq("test"),
                Mockito.eq(SagaSettings.buildDefault().toBuilder()
                    .availableAfterCompletionTimeout(Duration.ofSeconds(10))
                    .expirationTimeout(Duration.ofSeconds(20))
                    .build()
                )
            );
        }

        @Test
        void shouldOverrideAllWhenRegisterConfiguredSagas() {
            // when
            var sagaRegisterSettingsService = Mockito.mock(SagaRegisterSettingsService.class);
            var sagaPropertiesGroup = DistributedSagaProperties.SagaPropertiesGroup.builder()
                .defaultSagaProperties(
                    DistributedSagaProperties.SagaProperties.builder()
                        .availableAfterCompletionTimeout(Duration.ofSeconds(1))
                        .expirationTimeout(Duration.ofSeconds(2))
                        .stopOnFailedAnyRevert(true)
                        .build()
                )
                .sagaPropertiesGroup(Map.of(
                        "test",
                        DistributedSagaProperties.SagaProperties.builder()
                            .availableAfterCompletionTimeout(Duration.ofSeconds(10))
                            .expirationTimeout(Duration.ofSeconds(20))
                            .build()
                    )
                )
                .build();

            // do
            sagaPropertiesProcessor.registerConfiguredSagas(sagaRegisterSettingsService, sagaPropertiesGroup);

            // verify
            InOrder inOrder = Mockito.inOrder(sagaRegisterSettingsService);
            inOrder.verify(sagaRegisterSettingsService).registerDefaultSagaSettings(
                Mockito.eq(SagaSettings.builder()
                    .availableAfterCompletionTimeout(Duration.ofSeconds(1))
                    .expirationTimeout(Duration.ofSeconds(2))
                    .stopOnFailedAnyRevert(true)
                    .build()
                )
            );
            inOrder.verify(sagaRegisterSettingsService).registerSagaSettings(
                Mockito.eq("test"),
                Mockito.eq(SagaSettings.buildDefault().toBuilder()
                    .availableAfterCompletionTimeout(Duration.ofSeconds(10))
                    .expirationTimeout(Duration.ofSeconds(20))
                    .stopOnFailedAnyRevert(true)
                    .build()
                )
            );
        }
    }

    @Nested
    class BuildSagaMethodSettingsTest {
        //todo
    }
}