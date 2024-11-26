package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.configurations.SagaConfiguration;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.mappers.ContextMapper;
import com.distributed_task_framework.saga.models.SagaContext;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaContextRepository;
import com.distributed_task_framework.saga.services.SagaContextService;
import com.distributed_task_framework.service.DistributedTaskService;
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
import org.springframework.cache.annotation.Cacheable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaContextServiceImpl implements SagaContextService {
    DistributedTaskService distributedTaskService;
    SagaContextRepository sagaContextRepository;
    DlsSagaContextRepository dlsSagaContextRepository;
    SagaHelper sagaHelper;
    ContextMapper contextMapper;
    PlatformTransactionManager transactionManager;
    Clock clock;
    SagaConfiguration sagaConfiguration;
    ScheduledExecutorService scheduledExecutorService;

    public SagaContextServiceImpl(DistributedTaskService distributedTaskService,
                                  SagaContextRepository sagaContextRepository,
                                  DlsSagaContextRepository dlsSagaContextRepository,
                                  SagaHelper sagaHelper,
                                  ContextMapper contextMapper,
                                  PlatformTransactionManager transactionManager,
                                  Clock clock,
                                  SagaConfiguration sagaConfiguration) {
        this.distributedTaskService = distributedTaskService;
        this.sagaContextRepository = sagaContextRepository;
        this.dlsSagaContextRepository = dlsSagaContextRepository;
        this.sagaHelper = sagaHelper;
        this.clock = clock;
        this.contextMapper = contextMapper;
        this.transactionManager = transactionManager;
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
            sagaConfiguration.getContext().getDeprecatedSagaScanInitialDelay().toMillis(),
            sagaConfiguration.getContext().getDeprecatedSagaScanFixedDelay().toMillis(),
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
        handleCompletedSagas();
        handleExpiredSagas();
    }

    private void handleCompletedSagas() {
        SagaConfiguration.Context context = sagaConfiguration.getContext();

        var removedCompletedSagaContexts = sagaContextRepository.removeCompleted(context.getCompletedTimeout());
        if (!removedCompletedSagaContexts.isEmpty()) {
            log.info("handleCompletedSagas(): removedCompletedSagaContexts=[{}]", removedCompletedSagaContexts);
        }
    }

    private void handleExpiredSagas() {
        new TransactionTemplate(transactionManager)
            .executeWithoutResult(status -> {
                var expiredSagaContextEntities = sagaContextRepository.findExpired();
                if (expiredSagaContextEntities.isEmpty()) {
                    return;
                }
                var expiredSagaContexts = expiredSagaContextEntities.stream()
                    .map(contextMapper::toModel)
                    .toList();
                var expiredSagaContextIds = expiredSagaContexts.stream()
                    .map(SagaContext::getSagaId)
                    .toList();
                var expiredSagaRootTasksIds = expiredSagaContexts.stream()
                    .map(SagaContext::getRootTaskId)
                    .toList();
                log.info(
                    "handleHangingSagas(): expiredSagaContextIds=[{}], expiredSagaRootTasksIds=[{}]",
                    expiredSagaContextIds,
                    expiredSagaRootTasksIds
                );

                //todo: check potential deadlock ?
                try {
                    distributedTaskService.cancelAllWorkflowsByTaskId(expiredSagaRootTasksIds);
                } catch (Exception error) {
                    log.error("handleHangingSagas(): couldn't cancel taskIds=[{}]", expiredSagaRootTasksIds, error);
                    throw new RuntimeException(error);
                }

                var dlsSagaContextEntities = expiredSagaContextEntities.stream()
                    .map(contextMapper::mapToDls)
                    .toList();
                sagaContextRepository.removeAll(expiredSagaContextIds);
                dlsSagaContextRepository.saveOrUpdateAll(dlsSagaContextEntities);
            });
    }

    @Override
    public void create(SagaContext sagaContext) {
        var expirationTimeout = Optional.ofNullable(sagaConfiguration.getSagaPropertiesGroup().get(sagaContext.getName()))
            .map(SagaConfiguration.SagaProperties::getExpirationTimeout)
            .orElse(sagaConfiguration.getContext().getExpirationTimeout());
        var now = LocalDateTime.now(clock);
        var expiredDateUtc = now.plus(expirationTimeout);
        SagaContextEntity sagaContextEntity = contextMapper.toEntity(sagaContext).toBuilder()
            .createdDateUtc(now)
            .expirationDateUtc(expiredDateUtc)
            .build();
        sagaContextRepository.saveOrUpdate(sagaContextEntity);
    }

    @Override
    public void track(SagaEmbeddedPipelineContext context) {
        update(
            context.getSagaId(),
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .lastPipelineContext(contextMapper.sagaEmbeddedPipelineContextToByteArray(context))
                .build()
        );
    }

    @Override
    public SagaContext get(UUID sagaId) throws SagaNotFoundException {
        return sagaContextRepository.findById(sagaId)
            .map(contextMapper::toModel)
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
        update(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .completedDateUtc(LocalDateTime.now(clock))
                .build()
        );
    }

    @Override
    public void setOkResult(UUID sagaId, byte[] serializedValue) {
        update(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .result(serializedValue)
                .completedDateUtc(LocalDateTime.now(clock))
                .build()
        );
    }

    @Override
    public void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType) {
        update(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .result(serializedException)
                .exceptionType(exceptionType.toCanonical())
                .completedDateUtc(LocalDateTime.now(clock))
                .build()
        );
    }

    @Cacheable(cacheNames = "commonSagaCacheManager", key = "#root.args[0]")
    @Override
    public boolean isCompleted(UUID sagaId) {
        return sagaContextRepository.isCompleted(sagaId);
    }

    private void update(UUID sagaId, Function<SagaContextEntity, SagaContextEntity> updateAction) {
        sagaContextRepository.findById(sagaId)
            .map(updateAction)
            .ifPresentOrElse(
                sagaContextRepository::save,
                () -> log.warn("update(): saga context {} has been expired", sagaId)
            );
    }
}
