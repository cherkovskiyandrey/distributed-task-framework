package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.settings.Fixed;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.RetryMode;
import com.google.common.collect.Sets;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestSagaGenerator {
    private static final Set<String> IGNORE_METHOD_NAMES = Set.of("equals", "hashCode", "toString");
    private final Set<String> registeredSagas = Sets.newHashSet();
    private final Set<String> registeredSagaMethods = Sets.newHashSet();

    DistributionSagaService distributionSagaService;
    SagaResolver sagaResolver;

    public void reset() {
        registeredSagaMethods.forEach(distributionSagaService::unregisterSagaMethod);
        registeredSagaMethods.clear();
        registeredSagas.forEach(distributionSagaService::unregisterSagaSettings);
        registeredSagas.clear();
    }

    public <T> TestSagaModel<T> generateDefaultFor(T bean) {
        return generate(TestSagaModelSpec.builder(bean)
            .withRegisterAllMethods(true)
            .withMethodSettings(SagaMethodSettings.DEFAULT.toBuilder()
                .retry(Retry.builder()
                    .retryMode(RetryMode.FIXED)
                    .fixed(Fixed.builder()
                        .delay(Duration.ofMillis(100))
                        .maxNumber(1)
                        .build()
                    )
                    .build()
                )
                .build()
            )
            .build()
        );
    }

    public <T> TestSagaModel<T> generate(TestSagaModelSpec<T> testSagaModelSpec) {
        String sagaName = generateName(testSagaModelSpec);
        SagaSettings sagaSettings = generateSettings(testSagaModelSpec);
        distributionSagaService.registerSagaSettings(sagaName, sagaSettings);
        registeredSagas.add(sagaName);

        registerSagaMethods(testSagaModelSpec);

        return TestSagaModel.<T>builder()
            .name(sagaName)
            .bean(testSagaModelSpec.getBean())
            .sagaSettings(sagaSettings)
            .build();
    }

    private <T> void registerSagaMethods(TestSagaModelSpec<T> testSagaModelSpec) {
        Set<Method> alreadyRegisteredMethods = Sets.newHashSet();

        alreadyRegisteredMethods.addAll(registerSagaMethodBase(testSagaModelSpec.getFunctionMethods(), testSagaModelSpec));
        alreadyRegisteredMethods.addAll(registerSagaMethodBase(testSagaModelSpec.getBiFunctionMethods(), testSagaModelSpec));
        alreadyRegisteredMethods.addAll(registerSagaMethodBase(testSagaModelSpec.getConsumerMethods(), testSagaModelSpec));
        alreadyRegisteredMethods.addAll(registerSagaMethodBase(testSagaModelSpec.getSagaBiConsumerMethods(), testSagaModelSpec));

        registerAllMethodsIfRequired(testSagaModelSpec, alreadyRegisteredMethods);
    }

    private <T> void registerAllMethodsIfRequired(TestSagaModelSpec<T> testSagaModelSpec,
                                                  Set<Method> alreadyRegisteredMethods) {
        if (!Boolean.TRUE.equals(testSagaModelSpec.getRegisterAllMethods())) {
            return;
        }

        for (var method : testSagaModelSpec.getBean().getClass().getDeclaredMethods()) {
            if (alreadyRegisteredMethods.contains(method)) {
                continue;
            }
            if (IGNORE_METHOD_NAMES.contains(method.getName())) {
                continue;
            }
            var name = String.join("-", method.getName(), RandomStringUtils.randomAlphabetic(10));
            boolean isRevert = method.getAnnotation(Revert.class) != null;
            if (isRevert) {
                distributionSagaService.registerSagaRevertMethod(
                    name,
                    method,
                    testSagaModelSpec.getBean(),
                    generateSagaMethodSettings(null, testSagaModelSpec)
                );
            } else {
                distributionSagaService.registerSagaMethod(
                    name,
                    method,
                    testSagaModelSpec.getBean(),
                    generateSagaMethodSettings(null, testSagaModelSpec)
                );
            }
            registeredSagaMethods.add(name);
        }
    }

    private <U extends Serializable, T> Collection<Method> registerSagaMethodBase(Map<U, SagaMethodSettings> operationToSettings,
                                                                                  TestSagaModelSpec<T> testSagaModelSpec) {
        Set<Method> alreadyRegisteredMethods = Sets.newHashSet();
        for (var methodRef : operationToSettings.entrySet()) {
            var method = sagaResolver.resolveAsMethod(methodRef.getKey(), testSagaModelSpec.getBean());
            var name = String.join("-", method.getName(), RandomStringUtils.randomAlphabetic(10));
            boolean isRevert = method.getAnnotation(Revert.class) != null;
            if (isRevert) {
                distributionSagaService.registerSagaRevertMethod(
                    name,
                    method,
                    testSagaModelSpec.getBean(),
                    generateSagaMethodSettings(methodRef.getValue(), testSagaModelSpec)
                );
            } else {
                distributionSagaService.registerSagaMethod(
                    name,
                    method,
                    testSagaModelSpec.getBean(),
                    generateSagaMethodSettings(methodRef.getValue(), testSagaModelSpec)
                );
            }
            alreadyRegisteredMethods.add(method);
            registeredSagaMethods.add(name);
        }
        return alreadyRegisteredMethods;
    }

    private <T> SagaMethodSettings generateSagaMethodSettings(@Nullable SagaMethodSettings sagaMethodSettings,
                                                              TestSagaModelSpec<T> testSagaModelSpec) {
        return Optional.ofNullable(sagaMethodSettings)
            .orElseGet(() -> Optional.ofNullable(testSagaModelSpec.getMethodSettings())
                .orElse(SagaMethodSettings.DEFAULT)
            );
    }

    private <T> SagaSettings generateSettings(TestSagaModelSpec<T> testSagaModelSpec) {
        return Optional.ofNullable(testSagaModelSpec.getSagaSettings())
            .orElse(SagaSettings.DEFAULT);
    }

    private <T> String generateName(TestSagaModelSpec<T> testSagaModelSpec) {
        return Optional.ofNullable(testSagaModelSpec.getName())
            .orElse(RandomStringUtils.randomAlphabetic(30));
    }
}
