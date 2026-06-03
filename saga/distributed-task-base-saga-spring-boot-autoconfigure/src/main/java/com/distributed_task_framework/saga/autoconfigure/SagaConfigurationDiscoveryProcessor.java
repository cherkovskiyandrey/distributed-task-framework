package com.distributed_task_framework.saga.autoconfigure;

import com.distributed_task_framework.saga.autoconfigure.annotations.SagaMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.autoconfigure.annotations.SagaSpecific;
import com.distributed_task_framework.saga.autoconfigure.exceptions.SagaBeanInitException;
import com.distributed_task_framework.saga.autoconfigure.services.SagaPropertiesProcessor;
import com.distributed_task_framework.saga.autoconfigure.utils.ReflectionHelper;
import com.distributed_task_framework.saga.autoconfigure.utils.SagaNamingUtils;
import com.distributed_task_framework.saga.services.DistributionSagaService;
import com.distributed_task_framework.utils.DistributedTaskServiceLifecycle;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Don't implement BeanPostProcessor.
 * It leads to early creating of dependencies graph of service and MeterRegistry too.
 * As result a major of metrics will not be created. Usually injecting of MeterRegistry too
 * BeanPostProcessor is distinguished via {@link io.micrometer.core.instrument.binder.MeterBinder}
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaConfigurationDiscoveryProcessor implements DistributedTaskServiceLifecycle {
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

    ApplicationContext applicationContext;
    DistributionSagaService distributionSagaService;
    DistributedSagaProperties distributedSagaProperties;
    SagaPropertiesProcessor sagaPropertiesProcessor;
    Map<Object, ReflectionHelper.ProxyObject> beansToProxyObject = Maps.newIdentityHashMap();

    @Override
    public void init() {
        sagaPropertiesProcessor.registerConfiguredSagas(
            distributionSagaService,
            distributedSagaProperties.getSagaPropertiesGroup()
        );
    }

    /**
     * We register in this phase because only on this phase we have all singleton beans already created and initialised.
     * If bean is lazy created after spring context is started - we can't handle it. This behavior is by design:
     * it is dangerous to allow lazy beans, because can lead potentially to case when beans
     * will not be initialised and as result this node will not be able to handle corresponded DTF tasks.
     */
    @Override
    public void start() {
        forEachBean(bean -> {
                registerSagaMethodIfExists(bean);
                registerSagaRevertMethodIfExists(bean);
            }
        );
    }

    private void forEachBean(Consumer<Object> processor) {
        Arrays.stream(applicationContext.getBeanDefinitionNames())
            .filter(applicationContext::isSingleton)
            .map(applicationContext::getBean)
            .forEach(processor);
    }

    private void registerSagaMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
            .filter(this::isNotIgnoredMethod)
            .filter(method -> com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, SagaMethod.class).isPresent())
            .forEach(method -> {
                var suffix = buildSuffix(bean);
                var taskName = SagaNamingUtils.taskNameFor(method, suffix);
                var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                    method,
                    suffix,
                    distributedSagaProperties.getSagaMethodPropertiesGroup()
                );
                var proxyObject = beansToProxyObject.computeIfAbsent(bean, k -> ReflectionHelper.unwrapSpringBean(bean));

                distributionSagaService.registerSagaMethod(
                    taskName,
                    method,
                    proxyObject.targetObject(),
                    proxyObject.proxyWrappers(),
                    sagaMethodSettings
                );
            });
    }

    private void registerSagaRevertMethodIfExists(Object bean) {
        Arrays.stream(ReflectionUtils.getUniqueDeclaredMethods(AopUtils.getTargetClass(bean)))
            .filter(this::isNotIgnoredMethod)
            .filter(method -> com.distributed_task_framework.autoconfigure.utils.ReflectionHelper.findAnnotation(method, SagaRevertMethod.class).isPresent())
            .forEach(method -> {
                var suffix = buildSuffix(bean);
                var revertTaskName = SagaNamingUtils.taskNameFor(method, suffix);
                var sagaMethodSettings = sagaPropertiesProcessor.buildSagaMethodSettings(
                    method,
                    suffix,
                    distributedSagaProperties.getSagaMethodPropertiesGroup()
                );
                var proxyObject = beansToProxyObject.computeIfAbsent(bean, k -> ReflectionHelper.unwrapSpringBean(bean));

                distributionSagaService.registerSagaRevertMethod(
                    revertTaskName,
                    method,
                    proxyObject.targetObject(),
                    proxyObject.proxyWrappers(),
                    sagaMethodSettings
                );
            });
    }

    private boolean isNotIgnoredMethod(Method method) {
        return IGNORE_METHOD_NAMES.stream()
            .noneMatch(methodName -> method.getName().contains(methodName));
    }

    private String buildSuffix(Object bean) {
        String suffix = null;
        if (bean instanceof SagaSpecific sagaSpecific) {
            suffix = sagaSpecific.suffix();
            if (StringUtils.isBlank(suffix)) {
                throw new SagaBeanInitException("empty saga prefix for bean=[%s]".formatted(bean));
            }
        }
        return suffix;
    }
}
