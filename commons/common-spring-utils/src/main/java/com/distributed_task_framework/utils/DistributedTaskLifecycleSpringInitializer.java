package com.distributed_task_framework.utils;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * TODO: write description here
 *
 */
@Slf4j
public class DistributedTaskLifecycleSpringInitializer implements SmartLifecycle, BeanPostProcessor, ApplicationContextAware {
    private final AtomicReference<ApplicationContext> applicationContextRef;
    private final AtomicReference<SimpleSpringPhase> simpleSpringPhaseRef;
    private final List<SimpleServiceDefinition> orderedServices;
    private final ScheduledExecutorService distributedTaskStarterExecutorService;
    private volatile boolean isRunning;

    public DistributedTaskLifecycleSpringInitializer() {
        this.applicationContextRef = new AtomicReference<>();
        this.simpleSpringPhaseRef = new AtomicReference<>(SimpleSpringPhase.INIT);
        this.orderedServices = Lists.newCopyOnWriteArrayList();
        this.isRunning = false;
        this.distributedTaskStarterExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("dtf-lifecycle")
            .setDaemon(false)
            .setUncaughtExceptionHandler((t, e) ->
                log.error("Uncaught exception during handling dtf lifecycle", e)
            )
            .build()
        );
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        log.info("setApplicationContext(): application context is ready");
        applicationContextRef.set(applicationContext);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof DistributedTaskServiceLifecycle service) {
            initService(service, beanName);
            regService(beanName);
        }
        return bean;
    }

    private void regService(String beanName) {
        switch (simpleSpringPhaseRef.get()) {
            case INIT -> orderedServices.add(SimpleServiceDefinition.of(beanName));

            // for lazy beans
            case RUNNING -> {
                orderedServices.add(SimpleServiceDefinition.of(beanName));
                scheduleStartServices(100);
            }

            case DESTROY -> log.warn(
                "postProcessAfterInitialization(): don't invoke DistributedTaskServiceLifecycle#start() for bean [{}] in DESTROY phase",
                beanName
            );
        }
    }

    private void initService(DistributedTaskServiceLifecycle service, String beanName) {
        try {
            service.init();
            log.info("postProcessBeforeInitialization(): service [{}] has been inited successfully", beanName);
        } catch (Exception exception) {
            throw new BeanCreationException(
                "Error during DistributedTaskServiceLifecycle#init() for service=[%s]".formatted(beanName),
                exception
            );
        }
    }

    @Order(Ordered.LOWEST_PRECEDENCE)
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        log.info("onApplicationReady(): application is ready, schedule starting services...");
        simpleSpringPhaseRef.set(SimpleSpringPhase.RUNNING);
        // start services in spring running phase, after full spring context has been initialized
        scheduleStartServices(0);
    }

    private void scheduleStartServices(long delayMs) {
        distributedTaskStarterExecutorService.schedule(() -> {
                if (applicationContextRef.get() != null) {
                    startServices();
                } else {
                    log.warn(
                        "scheduleStartServices(): applicationContext hasn't been initialized yet, " +
                            "can't start services implemented DistributedTaskServiceLifecycle, " +
                            "try to reschedule starting."
                    );
                    scheduleStartServices(100); //todo:  cycle?
                }
            },
            delayMs,
            TimeUnit.MILLISECONDS
        );
    }

    private void startServices() {
        log.info("startServices(): starting");
        var applicationContext = applicationContextRef.get();

        for (int idx = 0; idx < orderedServices.size(); ++idx) {
            var serviceDefinition = orderedServices.get(idx);
            if (serviceDefinition.serviceState != SimpleServiceState.NOT_STARTED) {
                continue;
            }

            if (!checkConditionsToServiceStart(serviceDefinition.serviceName, applicationContext)) {
                return;
            }

            if (!startService(idx, serviceDefinition, applicationContext)) {
                return;
            }
        }
        log.info("startServices(): completed");
    }

    private boolean startService(int idx,
                                 SimpleServiceDefinition serviceDefinition,
                                 ApplicationContext applicationContext) {
        var serviceName = serviceDefinition.serviceName;
        try {
            var service = (DistributedTaskServiceLifecycle) applicationContext.getBean(serviceName);
            service.start();
            serviceDefinition = serviceDefinition.toBuilder()
                .serviceState(SimpleServiceState.STARTED)
                .build();

            orderedServices.set(idx, serviceDefinition);
            log.info("startServices(): service [{}] has been started successfully.", serviceName);
        } catch (Throwable throwable) {
            log.error(
                "startServices(): error during starting service: [{}]. Fail-stop strategy: stopping context...",
                serviceName,
                throwable
            );
            stopApplication(applicationContext);
            return false;
        }
        return true;
    }

    private boolean checkConditionsToServiceStart(String serviceName,
                                                  ApplicationContext applicationContext) {
        if (simpleSpringPhaseRef.get() != SimpleSpringPhase.RUNNING) {
            log.warn("startServices(): context is in [{}] phase, stop initializing beans!", simpleSpringPhaseRef.get());
            return false;
        }

        if (!applicationContext.containsBean(serviceName)) {
            log.warn(
                "startServices(): service by name [{}] doesn't exists in applicationContext and can't be started. " +
                    "May be is is a rase condition between BeanPostProcessor#postProcessAfterInitialization and " +
                    "startServices(). Try to reschedule starting.",
                serviceName
            );
            scheduleStartServices(100); //todo:  cycle?
            return false;
        }

        return true;
    }


    private void stopApplication(ApplicationContext applicationContext) {
        log.info("stopApplication(): going to stop application async");
        simpleSpringPhaseRef.set(SimpleSpringPhase.PRE_DESTROY);
        var threadFactory = new ThreadFactoryBuilder().setNameFormat("dtf-close").setDaemon(true).build();
        Executors.newSingleThreadExecutor(threadFactory).submit(() -> {
            try {
                SpringApplication.exit(applicationContext, () -> 1);
                System.exit(1);
            } catch (Throwable throwable) {
                log.error("Failed to stop spring application context gracefully", throwable);
                System.exit(1);
            }
        });
    }

    @Override
    public void start() {
        isRunning = true;
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
        shutdownDistributedTaskStarterExecutorService();
        stopServices();
        cleanupServices();
    }

    private void shutdownDistributedTaskStarterExecutorService() {
        log.info("shutdownDistributedTaskStarterExecutorService(): starting");
        distributedTaskStarterExecutorService.shutdown();
        try {
            if (!distributedTaskStarterExecutorService.awaitTermination(1, TimeUnit.MINUTES)) {
                log.error("Can't gracefully shutdown distributedTaskStarterExecutorService. Force to shutdown.");
                distributedTaskStarterExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("shutdownDistributedTaskStarterExecutorService(): completed");
    }

    private void stopServices() {
        log.info("stopServices(): starting");
        var size = orderedServices.size();
        for (int idx = size - 1; idx >= 0; idx--) {
            var serviceDefinition = orderedServices.get(idx);
            if (serviceDefinition.serviceState != SimpleServiceState.STARTED) {
                continue;
            }

            var serviceName = serviceDefinition.serviceName;
            try {
                ((DistributedTaskServiceLifecycle)applicationContextRef.get().getBean(serviceName)).stop();
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
            orderedServices.set(idx, serviceDefinition);
        }
        log.info("stopServices(): completed");
    }

    private void cleanupServices() {
        log.info("cleanupServices(): starting");
        var size = orderedServices.size();
        for (int idx = size - 1; idx >= 0; idx--) {
            var serviceDefinition = orderedServices.get(idx);
            if (!SimpleServiceState.STOPPED_SET.contains(serviceDefinition.serviceState)) {
                continue;
            }

            var serviceName = serviceDefinition.serviceName;
            try {
                ((DistributedTaskServiceLifecycle)applicationContextRef.get().getBean(serviceName)).cleanup();
                log.info("cleanupServices(): service [{}] has been cleaned successfully.", serviceName);
            } catch (Throwable throwable) {
                log.error("cleanupServices(): error during cleaned service: [{}]. Ignoring...", serviceName, throwable);
            }
            serviceDefinition = serviceDefinition.toBuilder()
                .serviceState(SimpleServiceState.CLEANED)
                .build();
            orderedServices.set(idx, serviceDefinition);
        }
        log.info("cleanupServices(): completed");
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE; // the earliest stop
    }

    private enum SimpleSpringPhase {
        INIT,
        RUNNING,
        PRE_DESTROY,
        DESTROY
    }

    private enum SimpleServiceState {
        NOT_STARTED,
        STARTED,
        STOPPED,
        STOPPED_FAIL,
        CLEANED;

        static final EnumSet<SimpleServiceState> STOPPED_SET = EnumSet.of(STOPPED, STOPPED_FAIL);
    }

    @Value
    @Builder(toBuilder = true)
    private static class SimpleServiceDefinition {
        String serviceName;
        @Builder.Default
        SimpleServiceState serviceState = SimpleServiceState.NOT_STARTED;

        static SimpleServiceDefinition of(String serviceName) {
            return SimpleServiceDefinition.builder().serviceName(serviceName).build();
        }
    }
}
