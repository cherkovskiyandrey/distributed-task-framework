package com.distributed_task_framework.utils;

import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.DestructionAwareBeanPostProcessor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

//todo: make a corrections of description

/**
 * Service to integrate DTF lifecycle into Spring lifecycle.
 * Rules:
 * <ol>
 *     <li>handling services which are implemented DistributedTaskServiceLifecycle in spring framework.</li>
 *     <li>handle eagle singleton beans.</li>
 *     <li>handle lazy singleton beans.</li>
 *     <li>doesn't handle prototype beans!</li>
 *     <li>{@link DistributedTaskServiceLifecycle#init()} is called in
 *     {@link BeanPostProcessor#postProcessBeforeInitialization(Object, String)} for each bean in scope of bean initialization</li>
 *     <li>{@link DistributedTaskServiceLifecycle#start()} is called in {@link SmartLifecycle#start()} with the lowest priority
 *     for all beans in spring default order</li>
 *     <li>if application context is already started, for lazy init or lazy beans {@link DistributedTaskServiceLifecycle#init()} and
 *     {@link DistributedTaskServiceLifecycle#start()} is called in {@link BeanPostProcessor#postProcessBeforeInitialization(Object, String)}</li>
 *     <li>in destroy phase service can be only {@link DistributedTaskServiceLifecycle#init()} and {@link DistributedTaskServiceLifecycle#cleanup()},
 *     {@link DistributedTaskServiceLifecycle#start()} and {@link DistributedTaskServiceLifecycle#stop()} are not invoked!</li>
 * </ol>
 *
 */
@Slf4j
public class DistributedTaskLifecycleSpringInitializer implements
    SmartLifecycle,
    DestructionAwareBeanPostProcessor,
    ApplicationContextAware {
    private final AtomicReference<SimpleSpringPhase> simpleSpringPhaseRef;
    private final LinkedHashMap<String, ServiceDefinition> orderedServicesToDeferredStart;
    private ConfigurableListableBeanFactory configurableListableBeanFactory;
    private volatile boolean isRunning;

    public DistributedTaskLifecycleSpringInitializer() {
        this.simpleSpringPhaseRef = new AtomicReference<>(SimpleSpringPhase.INIT);
        this.orderedServicesToDeferredStart = Maps.newLinkedHashMap();
        this.isRunning = false;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        log.info("setApplicationContext(): application context is ready");
        configurableListableBeanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (isApplicableService(bean, beanName)) {
            var service = (DistributedTaskServiceLifecycle) bean;
            initService(service, beanName);
            regAndStartServiceIfApplicable(service, beanName);
        }
        return bean;
    }

    private boolean isApplicableService(Object bean, String beanName) {
        if (!(bean instanceof DistributedTaskServiceLifecycle)) {
            return false;
        }

        var beanDefinition = configurableListableBeanFactory.getMergedBeanDefinition(beanName);
        if (!beanDefinition.isSingleton() || beanDefinition.isAbstract()) {
            log.warn(
                "isApplicableService(): service [{}] is implemented [{}] but isn't singleton or abstract! " +
                    "Will not be started. Only not abstract singleton services can implement DistributedTaskServiceLifecycle.",
                beanName,
                DistributedTaskServiceLifecycle.class.getSimpleName()
            );
            return false;
        }

        return true;
    }

    private void initService(DistributedTaskServiceLifecycle service, String beanName) {
        try {
            service.init();
            log.info("initService(): service [{}] has been initialized successfully", beanName);
        } catch (Exception exception) {
            throw new BeanCreationException(
                "Error during DistributedTaskServiceLifecycle#init() for service=[%s]".formatted(beanName),
                exception
            );
        }
    }

    private void regAndStartServiceIfApplicable(DistributedTaskServiceLifecycle service, String serviceName) {
        var phase = simpleSpringPhaseRef.get();
        switch (phase) {
            case INIT -> orderedServicesToDeferredStart.put(serviceName, ServiceDefinition.of(serviceName));

            // for lazy beans or lazy init beans
            case PRE_STARTED, RUNNING -> {
                var serviceWithState = ServiceDefinition.of(serviceName);
                serviceWithState = startService(service, serviceWithState);
                orderedServicesToDeferredStart.put(serviceName, serviceWithState);
            }

            case DESTROY -> log.warn(
                "regService(): don't invoke DistributedTaskServiceLifecycle#start() for bean [{}] in DESTROY phase",
                serviceName
            );
        }
    }

    @Override
    public void start() {
        simpleSpringPhaseRef.set(SimpleSpringPhase.PRE_STARTED);
        isRunning = true;
        // start singleton services synchronously in spring latest start LifeCycle phase,
        // after full spring context has been initialized, but application hasn't been started yet
        startServices();
    }

    @Order(Ordered.LOWEST_PRECEDENCE)
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        log.info("onApplicationReady(): application is ready");
        simpleSpringPhaseRef.set(SimpleSpringPhase.RUNNING);
    }

    private void startServices() {
        log.info("startServices(): starting");
        //take into account orderedServicesToDeferredStart can growth during traverse,
        //for example for lazy beans creating in start phase current bean,
        // that's why we fix currentSize in order not to traverse already started services again
        var fixedOrderedServicesToDeferredStart = Maps.newLinkedHashMap(orderedServicesToDeferredStart);
        for (var serviceDefinition : fixedOrderedServicesToDeferredStart.values()) {
            if (Objects.requireNonNull(serviceDefinition).serviceState != SimpleServiceState.NOT_STARTED) {
                continue;
            }

            if (!checkConditionsToServiceStart(serviceDefinition.serviceName)) {
                continue;
            }

            var service = (DistributedTaskServiceLifecycle) configurableListableBeanFactory.getBean(serviceDefinition.serviceName);
            serviceDefinition = startService(service, serviceDefinition);
            orderedServicesToDeferredStart.put(serviceDefinition.serviceName, serviceDefinition);
        }
        log.info("startServices(): completed");
    }

    private ServiceDefinition startService(DistributedTaskServiceLifecycle service,
                                           ServiceDefinition serviceDefinition) {
        var serviceName = serviceDefinition.serviceName;
        try {
            log.info("startServices(): going to start service [{}]", serviceName);
            service.start();
            log.info("startServices(): service [{}] has been started successfully.", serviceName);

            return serviceDefinition.toBuilder()
                .serviceState(SimpleServiceState.STARTED)
                .build();
        } catch (Throwable throwable) {
            log.error(
                "startServices(): error during starting service: [{}]. Stopping context...",
                serviceName,
                throwable
            );
            simpleSpringPhaseRef.set(SimpleSpringPhase.PRE_DESTROY);
            throw new BeanCreationException(
                "Error during DistributedTaskServiceLifecycle#start() for service=[%s]".formatted(serviceName),
                throwable
            );
        }
    }

    private boolean checkConditionsToServiceStart(String serviceName) {
        if (!SimpleSpringPhase.ALLOW_TO_START.contains(simpleSpringPhaseRef.get())) {
            log.warn("checkConditionsToServiceStart(): context is in [{}] phase, stopping to start service!", simpleSpringPhaseRef.get());
            return false;
        }

        if (!configurableListableBeanFactory.containsBean(serviceName)) {
            log.warn(
                "checkConditionsToServiceStart(): service by name [{}] doesn't exists in applicationContext and can't be started.",
                serviceName
            );
            return false;
        }
        return true;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void stop(Runnable callback) {
        simpleSpringPhaseRef.set(SimpleSpringPhase.DESTROY);
        try {
            stop();
        } finally {
            isRunning = false;
            callback.run();
        }
    }

    @Override
    public void stop() {
        var reverseOrderedServicesToStop = SpringGraphHelper.singletonTopologyReverseOrderByType(
            configurableListableBeanFactory,
            DistributedTaskServiceLifecycle.class
        );
        stopServices(reverseOrderedServicesToStop);
    }

    // don't protect orderedServicesToDeferredStart by creating of copy because of in stop phase, that's why
    // orderedServicesToDeferredStart will not be able to change
    private void stopServices(List<SpringGraphHelper.SimpleServiceDefinition<DistributedTaskServiceLifecycle>> reverseOrderedServicesToStop) {
        log.info("stopServices(): starting");
        for (var simpleServiceDefinition : reverseOrderedServicesToStop) {
            var serviceDefinition = orderedServicesToDeferredStart.get(simpleServiceDefinition.getServiceName());
            if (serviceDefinition == null) {
                log.error(
                    "stopServices(): can't stop service [{}] because it hasn't been registered " +
                        "in orderedServicesToDeferredStart, but exists in spring graph to stop",
                    simpleServiceDefinition.getServiceName()
                );
                continue;
            }
            if (serviceDefinition.serviceState != SimpleServiceState.STARTED) {
                log.warn(
                    "stopServices(): can't stop service [{}] because it is in [{}] state",
                    serviceDefinition.serviceName,
                    serviceDefinition.serviceState
                );
                continue;
            }

            var serviceName = simpleServiceDefinition.getServiceName();
            try {
                simpleServiceDefinition.getService().stop();
                serviceDefinition = serviceDefinition.toBuilder()
                    .serviceState(SimpleServiceState.STOPPED)
                    .build();
                log.info("stopServices(): service [{}] has been stopped successfully.", serviceName);
            } catch (Throwable throwable) {
                log.error("stopServices(): error during stopping service: [{}]. Ignoring...", serviceName, throwable);
                serviceDefinition = serviceDefinition.toBuilder()
                    .serviceState(SimpleServiceState.STOPPED_FAIL)
                    .build();
            }
            orderedServicesToDeferredStart.put(serviceName, serviceDefinition);
        }
        log.info("stopServices(): completed");
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void postProcessBeforeDestruction(Object bean, String beanName) throws BeansException {
        cleanupService((DistributedTaskServiceLifecycle) bean, beanName);
    }

    @Override
    public boolean requiresDestruction(Object bean) {
        return bean instanceof DistributedTaskServiceLifecycle;
    }

    private void cleanupService(DistributedTaskServiceLifecycle service, String serviceName) {
        log.info("cleanupService(): starting for {}", serviceName);
        var serviceWithState = orderedServicesToDeferredStart.get(serviceName);
        if (!SimpleServiceState.IN_STOPPED_PHASES.contains(serviceWithState.serviceState)) {
            return;
        }
        try {
            service.cleanup();
            log.info("cleanupService(): service [{}] has been cleaned successfully.", serviceName);
        } catch (Throwable throwable) {
            log.error("cleanupService(): error during cleaned service: [{}]. Ignoring...", serviceName, throwable);
        }
        serviceWithState = serviceWithState.toBuilder()
            .serviceState(SimpleServiceState.CLEANED)
            .build();
        orderedServicesToDeferredStart.put(serviceName, serviceWithState);
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public int getPhase() {
        return DEFAULT_PHASE; // the latest start and the earliest stop
    }

    private enum SimpleSpringPhase {
        INIT,
        PRE_STARTED,
        RUNNING,
        PRE_DESTROY,
        DESTROY;

        static final EnumSet<SimpleSpringPhase> ALLOW_TO_START = EnumSet.of(PRE_STARTED, RUNNING);
    }

    private enum SimpleServiceState {
        NOT_STARTED,
        STARTED,
        STOPPED,
        STOPPED_FAIL,
        CLEANED;

        static final EnumSet<SimpleServiceState> IN_STOPPED_PHASES = EnumSet.of(STOPPED, STOPPED_FAIL);
    }

    @Value
    @Builder(toBuilder = true)
    private static class ServiceDefinition {
        String serviceName;
        @EqualsAndHashCode.Exclude
        @Builder.Default
        SimpleServiceState serviceState = SimpleServiceState.NOT_STARTED;

        static ServiceDefinition of(String serviceName) {
            return ServiceDefinition.builder().serviceName(serviceName).build();
        }
    }
}
