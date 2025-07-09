package com.distributed_task_framework.saga.autoconfigure.services.impl;

import com.distributed_task_framework.saga.autoconfigure.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.autoconfigure.DistributedSagaProperties;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.services.SagaRegisterSettingsService;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.settings.TaskSettings;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.distributed_task_framework.persistence.repository.DtfRepositoryConstants.DTF_TX_MANAGER;
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

        public static class TestSaga {

            @SagaMethod(name = "sagaMethod")
            public void sagaMethod() {
            }

            @Transactional(transactionManager = DTF_TX_MANAGER)
            public void sagaMethodWithExactlyOnce() {
            }

            @SagaMethod(name = "sagaMethod", version = 1)
            public void sagaMethodWithVersion() {
            }

            @SagaMethod(name = "sagaMethodWithIgnoreException", noRetryFor = SagaExecutionException.class)
            public void sagaMethodWithIgnoreException() {
            }

        }

        @Test
        @SneakyThrows
        void shouldUseDefaultWhenBuildSagaMethodSettings() {
            //when & do
            var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                TestSaga.class.getMethod("sagaMethod"),
                null,
                null
            );

            //verify
            assertThat(sagaMethodSettings).isEqualTo(SagaMethodSettings.buildDefault());
        }

        @Test
        @SneakyThrows
        void shouldSetExactlyOnceWhenBuildSagaMethodSettingsAndTransactional() {
            //when & do
            var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                TestSaga.class.getMethod("sagaMethodWithExactlyOnce"),
                null,
                null
            );

            //verify
            assertThat(sagaMethodSettings).isEqualTo(SagaMethodSettings.buildDefault().toBuilder()
                .executionGuarantees(TaskSettings.ExecutionGuarantees.EXACTLY_ONCE)
                .build()
            );
        }

        @Test
        @SneakyThrows
        void shouldUseVersionWhenBuildSagaMethodSettings() {
            //when & do
            var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                TestSaga.class.getMethod("sagaMethodWithVersion"),
                null,
                DistributedSagaProperties.SagaMethodPropertiesGroup.builder()
                    .sagaMethodProperties(Map.of(
                            "sagaMethod",
                            DistributedSagaProperties.SagaMethodProperties.builder()
                                .maxParallelInCluster(999)
                                .build(),
                            "sagaMethod_v1",
                            DistributedSagaProperties.SagaMethodProperties.builder()
                                .maxParallelInCluster(1000)
                                .build()
                        )
                    )
                    .build()
            );

            //verify
            assertThat(sagaMethodSettings).isEqualTo(SagaMethodSettings.buildDefault().toBuilder()
                .maxParallelInCluster(1000)
                .build()
            );
        }

        @Test
        @SneakyThrows
        void shouldUseIgnoreExceptionWhenBuildSagaMethodSettings() {
            //when & do
            var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                TestSaga.class.getMethod("sagaMethodWithIgnoreException"),
                null,
                null
            );

            //verify
            assertThat(sagaMethodSettings).isEqualTo(SagaMethodSettings.buildDefault().toBuilder()
                .noRetryFor(List.of(SagaExecutionException.class))
                .build()
            );
        }

        //todo
    }
}