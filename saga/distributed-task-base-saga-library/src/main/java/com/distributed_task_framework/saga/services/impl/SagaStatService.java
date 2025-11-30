package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.persistence.entities.AggregatedSagaStat;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.saga.settings.SagaStatSettings;
import com.distributed_task_framework.service.internal.DistributedTaskMetricHelper;
import com.distributed_task_framework.service.internal.PlannerService;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaStatService {
    PlannerService plannerService;
    MeterRegistry meterRegistry;
    SagaRepository sagaRepository;
    SagaStatSettings sagaStatSettings;
    AtomicReference<ImmutableMap<AggregatedSagaStat.State, Integer>> aggregatedStatRef;
    Map<String, Meter.Id> metricNameToMetricId;
    Table<String, String, Meter.Id> metricNameToSagaNameToMetricId;
    Duration timeToClean;
    Timer aggregatedSagaStatCalculationTimer;
    Timer aggregatedSagaTopNStatCalculationTimer;
    String allSagasGaugeName;
    String activeSagasGaugeName;
    String completedSagasGaugeName;
    String completedCleaningSagasGaugeName;
    String completedNotCleanedSagasGaugeName;
    String expiredSagasGaugeName;
    String expiredNotCleanedSagasGaugeName;
    String undefinedSagasGaugeName;
    String activeTopNSagasGaugeName;
    String completedTopNSagasGaugeName;
    String completedTopNCleaningSagasGaugeName;
    String completedTopNNotCleanedSagasGaugeName;
    String expiredTopNSagasGaugeName;
    String expiredTopNNotCleanedSagasGaugeName;
    String undefinedTopNSagasGaugeName;
    ScheduledExecutorService executorService;

    public SagaStatService(PlannerService plannerService,
                           DistributedTaskMetricHelper distributedTaskMetricHelper,
                           MeterRegistry meterRegistry,
                           SagaRepository sagaRepository,
                           SagaCommonSettings sagaCommonSettings,
                           SagaStatSettings sagaStatSettings) {
        this.plannerService = plannerService;
        this.meterRegistry = meterRegistry;
        this.sagaRepository = sagaRepository;
        this.sagaStatSettings = sagaStatSettings;
        this.aggregatedStatRef = new AtomicReference<>(ImmutableMap.of());
        this.metricNameToMetricId = Maps.newHashMap();
        this.metricNameToSagaNameToMetricId = HashBasedTable.create();
        this.timeToClean = Duration.ofSeconds(sagaCommonSettings.getDeprecatedSagaScanFixedDelay().toSeconds() * 2);
        this.aggregatedSagaStatCalculationTimer = distributedTaskMetricHelper.timer("aggregatedSagaStatCalculationTimer");
        this.aggregatedSagaTopNStatCalculationTimer = distributedTaskMetricHelper.timer("aggregatedSagaTopNStatCalculationTimer");
        this.allSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "all");
        this.activeSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "active");
        this.completedSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "completed");
        this.completedCleaningSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "completed_cleaning");
        this.completedNotCleanedSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "completed_not_cleaned");
        this.expiredSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "expired");
        this.expiredNotCleanedSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "expired_not_cleaned");
        this.undefinedSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "undefined");
        this.activeTopNSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "topn", "active");
        this.completedTopNSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed");
        this.completedTopNCleaningSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed_cleaning");
        this.completedTopNNotCleanedSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "topn", "completed_not_cleaned");
        this.expiredTopNSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "topn", "expired");
        this.expiredTopNNotCleanedSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "topn", "expired_not_cleaned");
        this.undefinedTopNSagasGaugeName = distributedTaskMetricHelper.buildName("saga", "state", "topn", "undefined");
        this.executorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("vq-stat")
                .setUncaughtExceptionHandler((t, e) -> {
                    log.error("sagaStatService(): error trying to calculate stat", e);
                    ReflectionUtils.rethrowRuntimeException(e);
                })
                .build()
        );
    }

    @PostConstruct
    public void init() {
        executorService.scheduleWithFixedDelay(
            ExecutorUtils.wrapRepeatableRunnable(this::calculateStat),
            sagaStatSettings.getCalcInitialDelay().toMillis(),
            sagaStatSettings.getCalcFixedDelay().toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * @noinspection ResultOfMethodCallIgnored
     */
    @SneakyThrows
    @PreDestroy
    public void shutdown() {
        log.info("shutdown(): start of shutdown saga stat calculator");
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): completed shutdown saga stat calculator");
    }

    @VisibleForTesting
    void calculateStat() {
        if (plannerService.isActive()) {
            calculateAggregatedSagaStat();
            calculateAggregatedTopNSagaStat();
        } else {
            resetAggregatedSagaStat();
            resetAggregatedTopNSagaStat();
        }
    }

    @SuppressWarnings("DataFlowIssue")
    private void calculateAggregatedTopNSagaStat() {
        try {
            var aggregatedTopNStat = Objects.requireNonNull(aggregatedSagaTopNStatCalculationTimer.record(
                        () -> sagaRepository.getAggregatedTopNSagaStat(
                            sagaStatSettings.getTopNSagas(),
                            timeToClean
                        )
                    )
                ).stream()
                .collect(ImmutableListMultimap.toImmutableListMultimap(
                        AggregatedSagaStat::getState,
                        Function.identity()
                    )
                );

            registerStatMetric(activeTopNSagasGaugeName, aggregatedTopNStat.get(AggregatedSagaStat.State.ACTIVE));
            registerStatMetric(completedTopNSagasGaugeName, aggregatedTopNStat.get(AggregatedSagaStat.State.COMPLETED));
            registerStatMetric(completedTopNCleaningSagasGaugeName, aggregatedTopNStat.get(AggregatedSagaStat.State.COMPLETED_CLEANING));
            registerStatMetric(completedTopNNotCleanedSagasGaugeName, aggregatedTopNStat.get(AggregatedSagaStat.State.COMPLETED_NOT_CLEANED));
            registerStatMetric(expiredTopNSagasGaugeName, aggregatedTopNStat.get(AggregatedSagaStat.State.EXPIRED));
            registerStatMetric(expiredTopNNotCleanedSagasGaugeName, aggregatedTopNStat.get(AggregatedSagaStat.State.EXPIRED_NOT_CLEANED));
            registerStatMetric(undefinedTopNSagasGaugeName, aggregatedTopNStat.get(AggregatedSagaStat.State.UNDEFINED));
        } catch (Exception e) {
            log.error("calculateAggregatedTopNSagaStat(): can't be calculated aggregated topN statistic", e);
        }
    }

    private void registerStatMetric(String name, List<AggregatedSagaStat> stats) {
        resetAggregatedTopNSagaStat(name, stats);

        stats.forEach(stat -> {
                var meterId = Gauge.builder(
                        name,
                        stat::getNumber
                    )
                    .tag("saga_name", Objects.requireNonNull(stat.getName()))
                    .register(meterRegistry);
                metricNameToSagaNameToMetricId.put(name, stat.getName(), meterId.getId());
            }
        );
    }

    private void resetAggregatedTopNSagaStat(String name, List<AggregatedSagaStat> stats) {
        var sagaNameToMetricId = metricNameToSagaNameToMetricId.row(name);
        var sagaNamesToStat = stats.stream().map(AggregatedSagaStat::getName).collect(Collectors.toSet());
        var sagaNamesToRemove = Sets.newHashSet(Sets.difference(sagaNameToMetricId.keySet(), sagaNamesToStat));

        sagaNamesToRemove.forEach(sagaName ->
            meterRegistry.remove(Objects.requireNonNull(sagaNameToMetricId.get(sagaName)))
        );
    }

    @SuppressWarnings("DataFlowIssue")
    void calculateAggregatedSagaStat() {
        try {
            var aggregatedStat = Objects.requireNonNull(aggregatedSagaStatCalculationTimer.record(
                        () -> sagaRepository.getAggregatedSagaStat(timeToClean)
                    )
                ).stream()
                .collect(ImmutableMap.toImmutableMap(
                        AggregatedSagaStat::getState,
                        AggregatedSagaStat::getNumber
                    )
                );
            aggregatedStatRef.set(aggregatedStat);

            registerStatMetricsIfRequired();
        } catch (Exception e) {
            log.error("calculateAggregatedSagaStat(): can't be calculated aggregated statistic", e);
        }
    }

    private void registerStatMetricsIfRequired() {
        registerStatMetric(activeSagasGaugeName, AggregatedSagaStat.State.ACTIVE);
        registerStatMetric(completedSagasGaugeName, AggregatedSagaStat.State.COMPLETED);
        registerStatMetric(completedCleaningSagasGaugeName, AggregatedSagaStat.State.COMPLETED_CLEANING);
        registerStatMetric(completedNotCleanedSagasGaugeName, AggregatedSagaStat.State.COMPLETED_NOT_CLEANED);
        registerStatMetric(expiredSagasGaugeName, AggregatedSagaStat.State.EXPIRED);
        registerStatMetric(expiredNotCleanedSagasGaugeName, AggregatedSagaStat.State.EXPIRED_NOT_CLEANED);
        registerStatMetric(undefinedSagasGaugeName, AggregatedSagaStat.State.UNDEFINED);

        metricNameToMetricId.computeIfAbsent(allSagasGaugeName, k -> Gauge.builder(
                    allSagasGaugeName,
                    () -> aggregatedStatRef.get().values().stream().mapToInt(i -> i).sum()
                ).register(meterRegistry)
                .getId()
        );
    }

    private void registerStatMetric(String name, AggregatedSagaStat.State state) {
        metricNameToMetricId.computeIfAbsent(name, k ->
            Gauge.builder(
                    name,
                    () -> aggregatedStatRef.get().getOrDefault(state, 0)
                ).register(meterRegistry)
                .getId()
        );
    }

    private void resetAggregatedSagaStat() {
        metricNameToMetricId.values().forEach(meterRegistry::remove);
        metricNameToMetricId.clear();
    }

    @SuppressWarnings("DataFlowIssue")
    private void resetAggregatedTopNSagaStat() {
        metricNameToSagaNameToMetricId.columnMap().values()
            .forEach(stringIdMap -> stringIdMap.values().forEach(meterRegistry::remove));
    }
}
