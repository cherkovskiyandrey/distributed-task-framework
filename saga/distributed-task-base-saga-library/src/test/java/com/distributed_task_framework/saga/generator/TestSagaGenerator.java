package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.impl.SagaTask;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
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
import org.mockito.Mockito;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestSagaGenerator {
    private static final Set<String> IGNORE_METHOD_NAMES = Set.of("equals", "hashCode", "toString");
    private final Set<String> registeredSagas = Sets.newHashSet();
    private final Set<String> registeredSagaMethods = Sets.newHashSet();

    DistributionSagaService distributionSagaService;
    SagaResolver sagaResolver;
    SagaTaskFactory sagaTaskFactory;

    public void reset() {
        registeredSagaMethods.forEach(distributionSagaService::unregisterSagaMethod);
        registeredSagaMethods.clear();
        registeredSagas.forEach(distributionSagaService::unregisterSagaSettings);
        registeredSagas.clear();
    }

    public <T> TestSagaModel<T> generateDefaultFor(T bean) {
        return generate(TestSagaModelSpec.builder(bean)
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
        SagaSettings sagaSettings = generateSettingsAndRegisterIfRequired(sagaName, testSagaModelSpec);
        registeredSagas.add(sagaName);

        registerBeforeExecutionHooks(testSagaModelSpec);
        registerSagaMethods(testSagaModelSpec);

        return TestSagaModel.<T>builder()
            .name(sagaName)
            .bean(testSagaModelSpec.getBean())
            .sagaSettings(sagaSettings)
            .build();
    }

    private <T> void registerBeforeExecutionHooks(TestSagaModelSpec<T> testSagaModelSpec) {
        testSagaModelSpec.getBeforeExecution().forEach((sagaFunction, hook) -> {
                var method = sagaResolver.resolveAsMethod(sagaFunction, testSagaModelSpec.getBean());
                when(sagaTaskFactory.sagaTask(
                    argThat(taskDef -> taskDef.getTaskName().contains(method.getName() + "-")),
                    any(Method.class),
                    any(),
                    any(SagaMethodSettings.class))
                )
                    .thenAnswer(invocation -> {
                            var realTask = (SagaTask) invocation.callRealMethod();
                            var spyTask = Mockito.spy(realTask);
                            Mockito.doAnswer(inv -> {
                                        hook.execute();
                                        inv.callRealMethod();
                                        return null;
                                    }
                                )
                                .when(spyTask).execute(any(), any());
                            return spyTask;
                        }
                    );
            }
        );
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
        if (testSagaModelSpec.isDisableRegisterAllMethods()) {
            return;
        }

        for (var method : testSagaModelSpec.getBean().getClass().getMethods()) {
            if (alreadyRegisteredMethods.contains(method)) {
                continue;
            }
            if (IGNORE_METHOD_NAMES.contains(method.getName())) {
                continue;
            }
            var name = String.join("-", method.getName(), RandomStringUtils.randomAlphabetic(10));
            boolean isRevert = AnnotatedElementUtils.hasAnnotation(method, Revert.class);
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

    private <T> SagaSettings generateSettingsAndRegisterIfRequired(String sagaName,
                                                                   TestSagaModelSpec<T> testSagaModelSpec) {
        if (Boolean.TRUE.equals(testSagaModelSpec.getWithoutSettings())) {
            return null;
        }
        var sagaSettings = Optional.ofNullable(testSagaModelSpec.getSagaSettings())
            .orElse(SagaSettings.DEFAULT);
        distributionSagaService.registerSagaSettings(sagaName, sagaSettings);
        return sagaSettings;
    }

    private <T> String generateName(TestSagaModelSpec<T> testSagaModelSpec) {
        return Optional.ofNullable(testSagaModelSpec.getName())
            .orElse(RandomStringUtils.randomAlphabetic(30));
    }
}
