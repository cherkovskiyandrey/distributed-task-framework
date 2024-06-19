package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.configurations.SagaConfiguration;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.persistence.entities.SagaResultEntity;
import com.distributed_task_framework.saga.persistence.repository.SagaResultRepository;
import com.distributed_task_framework.saga.services.SagaResultService;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaResultServiceImpl implements SagaResultService {
    SagaResultRepository sagaResultRepository;
    SagaHelper sagaHelper;
    Clock clock;
    SagaConfiguration sagaConfiguration;
    ScheduledExecutorService scheduledExecutorService;

    public SagaResultServiceImpl(SagaResultRepository sagaResultRepository,
                                 SagaHelper sagaHelper,
                                 Clock clock,
                                 SagaConfiguration sagaConfiguration) {
        this.sagaResultRepository = sagaResultRepository;
        this.sagaHelper = sagaHelper;
        this.clock = clock;
        this.sagaConfiguration = sagaConfiguration;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("saga-result")
                .setUncaughtExceptionHandler((t, e) -> {
                    log.error("SagaResultServiceImpl(): unexpected error", e);
                    ReflectionUtils.rethrowRuntimeException(e);
                })
                .build()
        );
    }

    @PostConstruct
    public void init() {
        scheduledExecutorService.scheduleWithFixedDelay(
                ExecutorUtils.wrapRepeatableRunnable(this::handleDeprecatedResults),
                sagaConfiguration.getResult().getResultScanInitialDelay().toMillis(),
                sagaConfiguration.getResult().getResultScanFixedDelay().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): shutdown started");
        scheduledExecutorService.shutdownNow();
        scheduledExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): shutdown completed");
    }

    @VisibleForTesting
    void handleDeprecatedResults() {
        SagaConfiguration.Result result = sagaConfiguration.getResult();
        var removedExpiredEmptyResults = sagaResultRepository.removeExpiredEmptyResults(result.getEmptyResultDeprecationTimeout());
        if (!removedExpiredEmptyResults.isEmpty()) {
            log.info("handleDeprecatedResults(): removedExpiredEmptyResults=[{}]", removedExpiredEmptyResults);
        }

        var removeExpiredResults = sagaResultRepository.removeExpiredResults(result.getResultDeprecationTimeout());
        if (!removeExpiredResults.isEmpty()) {
            log.info("handleDeprecatedResults(): removeExpiredResults=[{}]", removeExpiredResults);
        }
    }

    @Override
    public void beginWatching(UUID sagaId) {
        sagaResultRepository.saveOrUpdate(SagaResultEntity.builder()
                .sagaId(sagaId)
                .createdDateUtc(LocalDateTime.now(clock))
                .build()
        );
    }

    @Override
    public <T> Optional<T> get(UUID sagaId, Class<T> resultType) throws SagaExecutionException {
        var sagaResultEntity = sagaResultRepository.findById(sagaId)
                .orElseThrow(() -> new SagaNotFoundException(
                        "Saga with id=[%s] doesn't exists or has been completed for a long time".formatted(sagaId))
                );
        var sagaResult = sagaResultEntity.getResult();
        if (sagaResult == null) {
            return Optional.empty();
        }

        if (StringUtils.isNotBlank(sagaResultEntity.getExceptionType())) {
            throw sagaHelper.buildExecutionException(sagaResultEntity.getExceptionType(), sagaResultEntity.getResult());
        }
        return sagaHelper.buildObject(sagaResult, resultType);
    }

    @Override
    public void setOkResult(UUID sagaId, byte[] serializedValue) {
        sagaResultRepository.findById(sagaId)
                .map(sagaResultEntity -> sagaResultEntity.toBuilder()
                        .result(serializedValue)
                        .completedDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .ifPresent(sagaResultRepository::save);
    }

    @Override
    public void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType) {
        sagaResultRepository.findById(sagaId)
                .map(sagaResultEntity -> sagaResultEntity.toBuilder()
                        .result(serializedException)
                        .exceptionType(exceptionType.toCanonical())
                        .completedDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .ifPresent(sagaResultRepository::save);
    }
}