package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.mappers.SagaMapper;
import com.distributed_task_framework.saga.models.CreateSagaRequest;
import com.distributed_task_framework.saga.models.Saga;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
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
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.ReflectionUtils;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaManagerImpl implements SagaManager {
    DistributedTaskService distributedTaskService;
    SagaRepository sagaRepository;
    DlsSagaContextRepository dlsSagaContextRepository;
    SagaHelper sagaHelper;
    SagaMapper sagaMapper;
    PlatformTransactionManager transactionManager;
    SagaCommonSettings sagaCommonSettings;
    Clock clock;
    ScheduledExecutorService scheduledExecutorService;

    public SagaManagerImpl(DistributedTaskService distributedTaskService,
                           SagaRepository sagaRepository,
                           DlsSagaContextRepository dlsSagaContextRepository,
                           SagaHelper sagaHelper,
                           SagaMapper sagaMapper,
                           PlatformTransactionManager transactionManager,
                           SagaCommonSettings sagaCommonSettings,
                           Clock clock) {
        this.distributedTaskService = distributedTaskService;
        this.sagaRepository = sagaRepository;
        this.dlsSagaContextRepository = dlsSagaContextRepository;
        this.sagaHelper = sagaHelper;
        this.clock = clock;
        this.sagaMapper = sagaMapper;
        this.transactionManager = transactionManager;
        this.sagaCommonSettings = sagaCommonSettings;
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
            sagaCommonSettings.getDeprecatedSagaScanInitialDelay().toMillis(),
            sagaCommonSettings.getDeprecatedSagaScanFixedDelay().toMillis(),
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
        var removedCompletedSagaContexts = sagaRepository.removeCompleted();
        if (!removedCompletedSagaContexts.isEmpty()) {
            log.info("handleCompletedSagas(): removedCompletedSagaContexts=[{}]", removedCompletedSagaContexts);
        }
    }

    private void handleExpiredSagas() {
        var transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.executeWithoutResult(status -> {
            var expiredSagaContextEntities = sagaRepository.findExpired();
            if (expiredSagaContextEntities.isEmpty()) {
                return;
            }

            var expiredSagaIds = expiredSagaContextEntities.stream()
                .map(SagaEntity::getSagaId)
                .toList();
            log.info("handleExpiredSagas(): expiredSagaIds=[{}]", expiredSagaIds);
            forceShutdown(expiredSagaContextEntities, true);
        });
    }

    private void forceShutdown(List<SagaEntity> sagaContextEntities, boolean moveToDls) {
        var transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.executeWithoutResult(status -> {
            if (sagaContextEntities.isEmpty()) {
                return;
            }
            var sagaToShutdownList = sagaContextEntities.stream()
                .map(sagaMapper::toModel)
                .toList();
            var sagaToShutdownIds = sagaToShutdownList.stream()
                .map(Saga::getSagaId)
                .toList();
            var sagaRootTasksIds = sagaToShutdownList.stream()
                .map(Saga::getRootTaskId)
                .toList();

            distributedTaskService.cancelAllWorkflowsByTaskId(sagaRootTasksIds);
            sagaRepository.removeAll(sagaToShutdownIds);

            if (moveToDls) {
                var dlsSagaContextEntities = sagaContextEntities.stream()
                    .map(sagaMapper::mapToDls)
                    .toList();
                dlsSagaContextRepository.saveOrUpdateAll(dlsSagaContextEntities);
            }

            log.info("forceShutdown(): sagaToShutdownIds=[{}], sagaRootTasksIds=[{}]", sagaToShutdownIds, sagaRootTasksIds);
        });
    }

    @Override
    public void create(CreateSagaRequest createSagaRequest, SagaSettings sagaSettings) {
        log.info("create(): createSagaRequest=[{}], sagaSettings=[{}]", createSagaRequest, sagaSettings);
        var now = LocalDateTime.now(clock);
        var expiredDateUtc = now.plus(sagaSettings.getExpirationTimeout());
        SagaEntity sagaEntity = sagaMapper.toEntity(createSagaRequest).toBuilder()
            .createdDateUtc(now)
            .expirationDateUtc(expiredDateUtc)
            .availableAfterCompletionTimeoutSec(sagaSettings.getAvailableAfterCompletionTimeout().toSeconds())
            .build();
        sagaRepository.saveOrUpdate(sagaEntity);
    }

    @Override
    public Saga get(UUID sagaId) throws SagaNotFoundException {
        return getIfExists(sagaId).orElseThrow(sagaNotFoundException(sagaId));
    }

    @Override
    public <T> Optional<T> getSagaResult(UUID sagaId, Class<T> resultType) throws
        SagaNotFoundException,
        SagaExecutionException,
        SagaCancellationException {
        var sagaResultEntity = sagaRepository.findById(sagaId)
            .orElseThrow(sagaNotFoundException(sagaId));

        if (sagaResultEntity.isCanceled()) {
            throw new SagaCancellationException(
                "Saga with id=[%s] has been gracefully canceled".formatted(sagaId)
            );
        }

        boolean isCompleted = sagaResultEntity.getCompletedDateUtc() != null;
        var sagaResult = sagaResultEntity.getResult();
        if (!isCompleted || sagaResult == null) {
            return Optional.empty();
        }

        if (StringUtils.isNotBlank(sagaResultEntity.getExceptionType())) {
            throw sagaHelper.buildExecutionException(sagaResultEntity.getExceptionType(), sagaResultEntity.getResult());
        }
        return sagaHelper.buildObject(sagaResult, resultType);
    }

    @Override
    public Optional<Saga> getIfExists(UUID sagaId) {
        return sagaRepository.findShortById(sagaId)
            .map(sagaMapper::toModel);
    }

    //because of exposed to client
    @Cacheable(cacheNames = "commonSagaCacheManager", key = "#root.args[0]")
    @Override
    public boolean isCompleted(UUID sagaId) throws SagaNotFoundException {
        return sagaRepository.isCompleted(sagaId)
            .orElseThrow(sagaNotFoundException(sagaId));
    }

    @Override
    public boolean isCanceled(UUID sagaId) {
        return sagaRepository.isCanceled(sagaId)
            .orElseThrow(sagaNotFoundException(sagaId));
    }

    @Override
    public void trackIfExists(SagaPipeline sagaPipeline) {
        log.info("trackIfExists(): sagaId=[{}], sagaPipeline=[{}]", sagaPipeline.getSagaId(), sagaPipeline);
        updateUnderLock(
            sagaPipeline.getSagaId(),
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .lastPipelineContext(sagaMapper.sagaEmbeddedPipelineContextToByteArray(sagaPipeline))
                .build(),
            false
        );
    }

    @Override
    public void completeIfExists(UUID sagaId) {
        log.info("completeIfExists(): sagaId=[{}]", sagaId);
        updateUnderLock(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .completedDateUtc(LocalDateTime.now(clock))
                .build(),
            false
        );
    }

    @Override
    public void setOkResultIfExists(UUID sagaId, byte[] serializedValue) {
        log.info("setOkResultIfExists(): sagaId=[{}]", sagaId);
        updateUnderLock(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .result(serializedValue)
                .build(),
            false
        );
    }

    @Override
    public void setFailResultIfExists(UUID sagaId, byte[] serializedException, JavaType exceptionType) {
        log.info("setFailResultIfExists(): sagaId=[{}], exceptionType=[{}]", sagaId, exceptionType);
        updateUnderLock(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .result(serializedException)
                .exceptionType(exceptionType.toCanonical())
                .build(),
            false
        );
    }

    @Override
    public void cancel(UUID sagaId) throws SagaNotFoundException {
        log.info("cancel(): cancel sagaId=[{}]", sagaId);
        updateUnderLock(
            sagaId,
            sagaEntity -> sagaEntity.toBuilder()
                .canceled(true)
                .build(),
            true
        );
    }

    @Override
    public void forceShutdown(UUID sagaId) {
        log.info("forceShutdown(): shutdown sagaId=[{}]", sagaId);
        sagaRepository.findById(sagaId)
            .ifPresentOrElse(
                //don't move shutdown sagas to DLS because it is explicit action
                sagaEntity -> forceShutdown(List.of(sagaEntity), false),
                () -> log.warn("forceShutdown(): saga=[{}] doesn't exists", sagaId)
            );
    }

    // NOTE: We use pessimistic update in order to protect from parallel changes, and
    // we will prepare to support parallel-saga mode. Parallel changes may be from client code,
    // for example cancellation and task any code from task.
    // Or for instance, in that mode SagaPipeline will be merged with existed in db before saving.
    private void updateUnderLock(UUID sagaId,
                                 Function<SagaEntity, SagaEntity> updateAction,
                                 boolean strictMode) throws SagaNotFoundException {
        var transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.executeWithoutResult(status -> {
                sagaRepository.findByIdIfExistsForUpdate(sagaId)
                    .map(updateAction)
                    .ifPresentOrElse(
                        sagaRepository::save,
                        () -> {
                            if (strictMode) {
                                throw sagaNotFoundException(sagaId).get();
                            }
                            log.warn("updateUnderLock(): saga {} doesn't exist", sagaId);
                        }
                    );
            }
        );
    }

    private Supplier<? extends RuntimeException> sagaNotFoundException(UUID sagaId) {
        return () -> new SagaNotFoundException(
            "Saga with id=[%s] doesn't exists or has been completed for a long time".formatted(sagaId)
        );
    }
}
