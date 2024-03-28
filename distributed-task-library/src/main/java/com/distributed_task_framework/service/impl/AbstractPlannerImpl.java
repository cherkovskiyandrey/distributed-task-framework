package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;
import com.distributed_task_framework.exception.OptimisticLockException;
import com.distributed_task_framework.persistence.entity.PlannerEntity;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.MetricHelper;
import com.distributed_task_framework.service.internal.PlannerService;
import com.distributed_task_framework.settings.CommonSettings;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PROTECTED)
public abstract class AbstractPlannerImpl implements PlannerService {
    CommonSettings commonSettings;
    PlannerRepository plannerRepository;
    PlatformTransactionManager transactionManager;
    ClusterProvider clusterProvider;
    ScheduledExecutorService watchdogExecutorService;
    ExecutorService plannerExecutorService;
    AtomicReference<Future<?>> planningLoopTask;
    MetricHelper metricHelper;
    List<Tag> commonTags;
    Timer planningTime;
    Counter optLockErrorCounter;
    Counter plannedTaskCounter;

    protected AbstractPlannerImpl(CommonSettings commonSettings,
                                  PlannerRepository plannerRepository,
                                  PlatformTransactionManager transactionManager,
                                  ClusterProvider clusterProvider,
                                  MetricHelper metricHelper) {
        this.commonSettings = commonSettings;
        this.plannerRepository = plannerRepository;
        this.transactionManager = transactionManager;
        this.clusterProvider = clusterProvider;
        this.planningLoopTask = new AtomicReference<>();
        this.metricHelper = metricHelper;
        this.commonTags = List.of(Tag.of("group", groupName()));
        this.planningTime = metricHelper.timer(List.of("planner", "time"), commonTags);
        this.optLockErrorCounter = metricHelper.counter(List.of("planner", "optlock", "error"), commonTags);
        this.plannedTaskCounter = metricHelper.counter(List.of("planner", "tasks", "planned"), commonTags);
        this.watchdogExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("shed-watch-%d")
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("scheduleWatchdog(): error when watch by schedule table", e);
                            ReflectionUtils.rethrowRuntimeException(e);
                        })
                        .build()
        );
        this.plannerExecutorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("planner-" + shortName() + "-%d")
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("planner(): error when run planning", e);
                            ReflectionUtils.rethrowRuntimeException(e);
                        })
                        .build()
        );
    }

    protected abstract String name();

    protected abstract String shortName();

    protected abstract String groupName();

    protected boolean hasToBeActive() {
        return true;
    }

    protected boolean inTransaction() {
        return false;
    }

    protected void beforeStartLoop() {
    }

    abstract int processInLoop();

    protected void afterStartLoop() {
    }

    @PostConstruct
    public void init() {
        watchdogExecutorService.scheduleWithFixedDelay(
                ExecutorUtils.wrapRepeatableRunnable(this::watchdog),
                commonSettings.getPlannerSettings().getWatchdogInitialDelayMs(),
                commonSettings.getPlannerSettings().getWatchdogFixedDelayMs(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * @noinspection ResultOfMethodCallIgnored
     */
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): \"{}\" shutdown started", name());
        watchdogExecutorService.shutdownNow();
        plannerExecutorService.shutdownNow();
        watchdogExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        plannerExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): \"{}\" shutdown completed", name());
        plannerRepository.deleteById(clusterProvider.nodeId());
    }

    @Override
    public boolean isActive() {
        return planningLoopTask.get() != null;
    }

    @VisibleForTesting
    void watchdog() {
        Future<?> planningLoopFuture = planningLoopTask.get();
        if (!hasToBeActive()) {
            if (planningLoopFuture != null) {
                log.info("watchdog(): \"{}\" planner has to be inactive. Stopping...", name());
                stopPlanner(planningLoopFuture);
            }
            return;
        }

        Optional<PlannerEntity> activePlannerOpt = plannerRepository.findByGroupName(groupName());
        if (activePlannerOpt.isPresent()) {
            PlannerEntity activePlanner = activePlannerOpt.get();
            if (clusterProvider.nodeId().equals(activePlanner.getNodeStateId())) {
                if (planningLoopFuture == null) {
                    log.warn("watchdog(): \"{}\" planner=[{}] hasn't been started right after was assigned, starting...",
                            name(), clusterProvider.nodeId()
                    );
                    startPlanner();
                } else if (planningLoopFuture.isDone()) {
                    log.error("watchdog(): \"{}\" planner=[{}] has been completed abnormally, restart it",
                            name(), clusterProvider.nodeId()
                    );
                    startPlanner();
                }
                //do nothing it is ok.
                return;
            }
            if (planningLoopFuture != null) {
                log.error("watchdog(): \"{}\" planner conflict detected: nodeId=[{}], concurrentPlannerNodeId=[{}]",
                        name(), clusterProvider.nodeId(), activePlanner.getNodeStateId()
                );
                stopPlanner(planningLoopFuture);
            }
            return;
        }
        log.info("watchdog(): {} can't detect active planner, try to become it", name());
        try {
            plannerRepository.save(PlannerEntity.builder()
                    .groupName(groupName())
                    .nodeStateId(clusterProvider.nodeId())
                    .build()
            );
        } catch (Exception exception) {
            log.info("watchdog(): \"{}\" nodeId=[{}] can't become an active planner", name(), clusterProvider.nodeId());
            return;
        }
        if (planningLoopTask.get() == null) {
            startPlanner();
            log.info("watchdog(): \"{}\" nodeId=[{}] is an active planner", name(), clusterProvider.nodeId());
        } else {
            log.warn("watchdog(): \"{}\" nodeId=[{}] is an active planner and has already started plannerLoop",
                    name(), clusterProvider.nodeId()
            );
        }
    }

    private void startPlanner() {
        planningLoopTask.set(plannerExecutorService.submit(ExecutorUtils.wrapRunnable(this::planningLoop)));
    }

    private void stopPlanner(Future<?> planningLoopFuture) {
        planningLoopFuture.cancel(true);
        planningLoopTask.set(null);
    }

    @SuppressWarnings("ConstantConditions")
    @VisibleForTesting
    void planningLoop() {
        log.info("planningLoop(): has been started for \"{}\"", name());
        beforeStartLoop();
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
long beginTime = System.currentTimeMillis();
                    int taskNumber = planningTime.recordCallable(() -> inTransaction() ?
                            transactionTemplate.execute(status -> processInLoop()) :
                            processInLoop()
                    );
//log.info("planningLoop(): \"{}\" time={}", name(), Duration.ofMillis(System.currentTimeMillis() - beginTime));

                    if (taskNumber > 0) {
                        plannedTaskCounter.increment(taskNumber);
                        log.info("planningLoop(): \"{}\", taskNumber=[{}]", name(), taskNumber);
                    }
                    sleep(taskNumber);
                } catch (OptimisticLockException exception) {
                    optLockErrorCounter.increment();
                    log.info("planningLoop(): \"{}\" concurrent access detected=[{}]", name(), exception.getMessage());
                } catch (InterruptedException e) {
                    log.info("planningLoop(): {} has been interrupted.", name());
                    return;
                } catch (Exception e) {
                    log.error("planningLoop(): \"{}\" ", name(), e);
                }
            }
        } finally {
            afterStartLoop();
        }
        log.info("planningLoop(): has been stopped");
    }

    /**
     * @noinspection ConstantConditions, UnstableApiUsage
     */
    private void sleep(int taskNumber) throws InterruptedException {
        Integer maxInConfig = commonSettings.getPlannerSettings().getPollingDelay().span().upperEndpoint();
        taskNumber = Math.min(taskNumber, maxInConfig);
        Integer delayMs = commonSettings.getPlannerSettings().getPollingDelay().get(taskNumber);
        if (delayMs > 0) {
//log.info("sleep(): \"{}\" gonna to sleep {} ms", name(), delayMs);
            TimeUnit.MILLISECONDS.sleep(delayMs);
        }
    }
}
