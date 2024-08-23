package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.configurations.SagaConfiguration;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.mappers.ContextMapper;
import com.distributed_task_framework.saga.models.SagaContext;
import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;
import com.distributed_task_framework.saga.persistence.repository.SagaContextRepository;
import com.distributed_task_framework.saga.services.SagaContextService;
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
public class SagaContextServiceImpl implements SagaContextService {
    SagaContextRepository sagaContextRepository;
    SagaHelper sagaHelper;
    ContextMapper contextMapper;
    Clock clock;
    SagaConfiguration sagaConfiguration;
    ScheduledExecutorService scheduledExecutorService;

    public SagaContextServiceImpl(SagaContextRepository sagaContextRepository,
                                  SagaHelper sagaHelper,
                                  ContextMapper contextMapper,
                                  Clock clock,
                                  SagaConfiguration sagaConfiguration) {
        this.sagaContextRepository = sagaContextRepository;
        this.sagaHelper = sagaHelper;
        this.clock = clock;
        this.contextMapper = contextMapper;
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
                ExecutorUtils.wrapRepeatableRunnable(this::handleDeprecatedSagas),
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
    void handleDeprecatedSagas() {
        SagaConfiguration.Result result = sagaConfiguration.getResult();

        //todo: think about this functionality: should we cancel all workflows before? or may be this is unnecessary action
        // and case is synthetic?
        var removedExpiredEmptyResults = sagaContextRepository.removeHanging(result.getEmptyResultDeprecationTimeout());
        if (!removedExpiredEmptyResults.isEmpty()) {
            log.info("handleDeprecatedResults(): removedExpiredEmptyResults=[{}]", removedExpiredEmptyResults);
        }

        var removeExpiredResults = sagaContextRepository.removeExpired(result.getResultDeprecationTimeout());
        if (!removeExpiredResults.isEmpty()) {
            log.info("handleDeprecatedResults(): removeExpiredResults=[{}]", removeExpiredResults);
        }
    }

    @Override
    public void create(UUID sagaId, TaskId taskId) {
        SagaContext sagaContext = SagaContext.builder()
                .sagaId(sagaId)
                .rootTaskId(taskId)
                .build();
        SagaContextEntity sagaContextEntity = contextMapper.map(sagaContext).toBuilder()
                .createdDateUtc(LocalDateTime.now(clock))
                .build();
        sagaContextRepository.saveOrUpdate(sagaContextEntity);
    }

    @Override
    public SagaContext get(UUID sagaId) throws SagaNotFoundException {
        return sagaContextRepository.findById(sagaId)
                .map(contextMapper::map)
                .orElseThrow(() -> new SagaNotFoundException(
                        "Saga with id=[%s] doesn't exists or has been completed for a long time".formatted(sagaId))
                );
    }

    @Override
    public <T> Optional<T> getSagaResult(UUID sagaId, Class<T> resultType) throws SagaExecutionException {
        var sagaResultEntity = sagaContextRepository.findById(sagaId)
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
    public void setCompleted(UUID sagaId) {
        sagaContextRepository.findById(sagaId)
                .map(sagaContextEntity -> sagaContextEntity.toBuilder()
                        .completedDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .ifPresent(sagaContextRepository::save);
    }

    @Override
    public void setOkResult(UUID sagaId, byte[] serializedValue) {
        sagaContextRepository.findById(sagaId)
                .map(sagaContextEntity -> sagaContextEntity.toBuilder()
                        .result(serializedValue)
                        .completedDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .ifPresent(sagaContextRepository::save);
    }

    @Override
    public void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType) {
        sagaContextRepository.findById(sagaId)
                .map(sagaContextEntity -> sagaContextEntity.toBuilder()
                        .result(serializedException)
                        .exceptionType(exceptionType.toCanonical())
                        .completedDateUtc(LocalDateTime.now(clock))
                        .build()
                )
                .ifPresent(sagaContextRepository::save);
    }
}
