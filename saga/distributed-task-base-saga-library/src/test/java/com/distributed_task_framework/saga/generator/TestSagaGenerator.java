package com.distributed_task_framework.saga.generator;

import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.saga.services.impl.SagaTask;
import com.distributed_task_framework.saga.services.internal.SagaResolver;
import com.distributed_task_framework.saga.services.internal.SagaTaskFactory;
import com.distributed_task_framework.saga.settings.SagaMethodSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.saga.utils.MethodSagaMethodFactory;
import com.google.common.collect.Sets;
import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.MockMakers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ReflectionUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TestSagaGenerator {

    private static final Set<String> IGNORE_METHOD_NAMES = Set.of(
        "equals",
        "hashCode",
        "toString",
        "wait",
        "notify",
        "getClass",
        "notifyAll",
        "finalize",
        "clone"
    );
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

    public <T> TestSagaModel<T> generateFor(T bean) {
        return generate(TestSagaModelSpec.builder(bean).build());
    }

    public <T> TestSagaModel<T> generate(TestSagaModelSpec<T> testSagaModelSpec) {
        testSagaModelSpec = registerAfterSagaMethodExecution(testSagaModelSpec);
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

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private <T> TestSagaModelSpec<T> registerAfterSagaMethodExecution(TestSagaModelSpec<T> testSagaModelSpec) {
        if (testSagaModelSpec.getAfterSagaMethodExecution().isEmpty()) {
            return testSagaModelSpec;
        }

        var originalBean = testSagaModelSpec.getBean();
        var sagaMethodToMethod = Arrays.stream(testSagaModelSpec.getBean().getClass().getMethods())
            .map(method -> {
                    method.setAccessible(true);
                    var sagaMethod = MethodSagaMethodFactory.of(method);
                    return Pair.of(sagaMethod, method);
                }
            )
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        var sagaMethodToHandler = testSagaModelSpec.getAfterSagaMethodExecution().entrySet().stream()
            .map(entry -> {
                    var method = sagaResolver.findMethodInObject(entry.getKey(), testSagaModelSpec.getBean());
                    var sagaMethod = MethodSagaMethodFactory.of(method);
                    return Pair.of(sagaMethod, entry.getValue());
                }
            )
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        var bean = (T) Mockito.mock(
            testSagaModelSpec.getBean().getClass(),
            withSettings()
                .name(testSagaModelSpec.getBean().getClass().getName() + "Mock")
                .mockMaker(MockMakers.INLINE)
                .verboseLogging()
                .defaultAnswer(
                    (Answer<Object>) invocation -> {
                        var args = invocation.getArguments();
                        var originalMethod = invocation.getMethod();
                        var originalSagaMethod = MethodSagaMethodFactory.of(originalMethod);

                        try {
                            return sagaMethodToMethod.get(originalSagaMethod).invoke(originalBean, args);
                        } finally {
                            var afterHandler = sagaMethodToHandler.get(originalSagaMethod);
                            if (afterHandler != null) {
                                afterHandler.execute();
                            }
                        }
                    }
                )
        );

        return testSagaModelSpec.toBuilder(bean).build();
    }

    private <T> void registerBeforeExecutionHooks(TestSagaModelSpec<T> testSagaModelSpec) {
        testSagaModelSpec.getBeforeTaskExecution().forEach((sagaFunction, hook) -> {
                var method = sagaResolver.findMethodInObject(sagaFunction, testSagaModelSpec.getBean());
                when(sagaTaskFactory.sagaTask(
                    argThat(taskDef -> taskDef.getTaskName().contains(method.getName() + "-")),
                    any(Method.class),
                    any(),
                    any(SagaMethodSettings.class))
                )
                    .thenAnswer(invocation -> {
                            var realTask = (SagaTask) invocation.callRealMethod();
                            var spyTask = Mockito.spy(realTask);
                            var calls = new AtomicInteger(0);
                            Mockito.doAnswer(inv -> {
                                        if (!hook.isLastCall() || calls.get() > 0) {
                                            hook.runnable().execute();
                                        }
                                        calls.getAndIncrement();
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
        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(testSagaModelSpec.getBean().getClass()))
            .filter(method -> !alreadyRegisteredMethods.contains(method))
            .filter(method -> !isIgnoredMethod(method))
            .forEach(method -> {
                    var name = String.join("-", method.getName(), RandomStringUtils.randomAlphabetic(10));
                    boolean isRevert = AnnotatedElementUtils.hasAnnotation(method, Revert.class);
                    if (isRevert) {
                        distributionSagaService.registerSagaRevertMethod(
                            name,
                            method,
                            testSagaModelSpec.getBean(),
                            List.of(),
                            generateSagaMethodSettings(null, testSagaModelSpec)
                        );
                    } else {
                        distributionSagaService.registerSagaMethod(
                            name,
                            method,
                            testSagaModelSpec.getBean(),
                            List.of(),
                            generateSagaMethodSettings(null, testSagaModelSpec)
                        );
                    }
                    registeredSagaMethods.add(name);
                }
            );
    }

    private boolean isIgnoredMethod(Method method) {
        return IGNORE_METHOD_NAMES.stream()
            .anyMatch(methodName -> method.getName().contains(methodName));
    }

    private <U extends Serializable, T> Collection<Method> registerSagaMethodBase(Map<U, SagaMethodSettings> operationToSettings,
                                                                                  TestSagaModelSpec<T> testSagaModelSpec) {
        Set<Method> alreadyRegisteredMethods = Sets.newHashSet();
        for (var methodRef : operationToSettings.entrySet()) {
            var method = sagaResolver.findMethodInObject(methodRef.getKey(), testSagaModelSpec.getBean());
            var name = String.join("-", method.getName(), RandomStringUtils.randomAlphabetic(10));
            boolean isRevert = AnnotatedElementUtils.hasAnnotation(method, Revert.class);
            if (isRevert) {
                distributionSagaService.registerSagaRevertMethod(
                    name,
                    method,
                    testSagaModelSpec.getBean(),
                    List.of(),
                    generateSagaMethodSettings(methodRef.getValue(), testSagaModelSpec)
                );
            } else {
                distributionSagaService.registerSagaMethod(
                    name,
                    method,
                    testSagaModelSpec.getBean(),
                    List.of(),
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
                .orElse(TestSagaGeneratorUtils.withRetry(1))
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
