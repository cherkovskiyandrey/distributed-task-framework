package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.saga.configurations.SagaConfiguration;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.mappers.ContextMapper;
import com.distributed_task_framework.saga.models.Saga;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaContextRepository;
import com.distributed_task_framework.saga.services.SagaManager;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaManagerImpl implements SagaManager {
    DistributedTaskService distributedTaskService;
    SagaContextRepository sagaContextRepository;
    DlsSagaContextRepository dlsSagaContextRepository;
    SagaHelper sagaHelper;
    ContextMapper contextMapper;
    PlatformTransactionManager transactionManager;
    Clock clock;
    SagaConfiguration sagaConfiguration;
    ScheduledExecutorService scheduledExecutorService;

    public SagaManagerImpl(DistributedTaskService distributedTaskService,
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
        var transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.executeWithoutResult(status -> {
            var expiredSagaContextEntities = sagaContextRepository.findExpired();
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
                .map(contextMapper::toModel)
                .toList();
            var sagaToShutdownIds = sagaToShutdownList.stream()
                .map(Saga::getSagaId)
                .toList();
            var sagaRootTasksIds = sagaToShutdownList.stream()
                .map(Saga::getRootTaskId)
                .toList();

            distributedTaskService.cancelAllWorkflowsByTaskId(sagaRootTasksIds);
            sagaContextRepository.removeAll(sagaToShutdownIds);

            if (moveToDls) {
                var dlsSagaContextEntities = sagaContextEntities.stream()
                    .map(contextMapper::mapToDls)
                    .toList();
                dlsSagaContextRepository.saveOrUpdateAll(dlsSagaContextEntities);
            }

            log.info("forceShutdown(): sagaToShutdownIds=[{}], sagaRootTasksIds=[{}]", sagaToShutdownIds, sagaRootTasksIds);
        });
    }

    @Override
    public void create(Saga sagaContext) {
        var expirationTimeout = Optional.ofNullable(sagaConfiguration.getSagaPropertiesGroup().get(sagaContext.getName()))
            .map(SagaConfiguration.SagaProperties::getExpirationTimeout)
            .orElse(sagaConfiguration.getContext().getExpirationTimeout());
        var now = LocalDateTime.now(clock);
        var expiredDateUtc = now.plus(expirationTimeout);
        SagaEntity sagaEntity = contextMapper.toEntity(sagaContext).toBuilder()
            .createdDateUtc(now)
            .expirationDateUtc(expiredDateUtc)
            .build();
        sagaContextRepository.saveOrUpdate(sagaEntity);
    }

    @Override
    public void track(SagaPipeline context) {
        update(
            context.getSagaId(),
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .lastPipelineContext(contextMapper.sagaEmbeddedPipelineContextToByteArray(context))
                .build()
        );
    }

    @Override
    public Saga get(UUID sagaId) throws SagaNotFoundException {
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

        if (sagaResultEntity.isCanceled()) {
            throw new CancellationException(
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
    public void complete(UUID sagaId) {
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
                .build()
        );
    }

    //todo: concurrent changes possible!!! use select for update
    @Override
    public void setFailResult(UUID sagaId, byte[] serializedException, JavaType exceptionType) {
        update(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .result(serializedException)
                .exceptionType(exceptionType.toCanonical())
                .build()
        );
    }

    //because of exposed to client
    @Cacheable(cacheNames = "commonSagaCacheManager", key = "#root.args[0]")
    @Override
    public boolean isCompleted(UUID sagaId) {
        return sagaContextRepository.isCompleted(sagaId);
    }

    //don't have a cache because of doesn't exposed to client
    @Override
    public boolean isCanceled(UUID sagaId) {
        return sagaContextRepository.isCanceled(sagaId);
    }

    //todo: the problem in concurrent!! use select for update!!!
    @Override
    public void cancel(UUID sagaId) {
        log.info("cancel(): cancel sagaId=[{}]", sagaId);
        update(
            sagaId,
            sagaEntity -> sagaEntity.toBuilder()
                .canceled(true)
                .build()
        );
    }

    @Override
    public void forceShutdown(UUID sagaId) {
        log.info("forceShutdown(): shutdown sagaId=[{}]", sagaId);
        sagaContextRepository.findById(sagaId)
            .ifPresentOrElse(
                //don't move shutdown sagas to DLS because it is explicit action
                sagaEntity -> forceShutdown(List.of(sagaEntity), false),
                () -> log.warn("forceShutdown(): saga=[{}] doesn't exists", sagaId)
            );
    }

    private void update(UUID sagaId, Function<SagaEntity, SagaEntity> updateAction) {
        sagaContextRepository.findById(sagaId)
            .map(updateAction)
            .ifPresentOrElse(
                sagaContextRepository::save,
                () -> log.warn("update(): saga context {} has been expired", sagaId)
            );
    }
}
