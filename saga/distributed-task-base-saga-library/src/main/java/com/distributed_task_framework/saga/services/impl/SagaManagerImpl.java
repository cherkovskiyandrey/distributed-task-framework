package com.distributed_task_framework.saga.services.impl;

import com.distributed_task_framework.model.ExecutionContext;
import com.distributed_task_framework.model.TaskDef;
import com.distributed_task_framework.saga.exceptions.SagaCancellationException;
import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.mappers.SagaMapper;
import com.distributed_task_framework.saga.models.CreateSagaRequest;
import com.distributed_task_framework.saga.models.Saga;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
import com.distributed_task_framework.saga.persistence.entities.ShortSagaEntity;
import com.distributed_task_framework.saga.persistence.repository.DlsSagaContextRepository;
import com.distributed_task_framework.saga.persistence.repository.SagaRepository;
import com.distributed_task_framework.saga.services.internal.SagaManager;
import com.distributed_task_framework.saga.settings.SagaCommonSettings;
import com.distributed_task_framework.saga.settings.SagaSettings;
import com.distributed_task_framework.service.DistributedTaskService;
import com.distributed_task_framework.settings.Retry;
import com.distributed_task_framework.settings.RetryMode;
import com.distributed_task_framework.settings.TaskSettings;
import com.distributed_task_framework.utils.DistributedTaskCache;
import com.distributed_task_framework.utils.DistributedTaskCacheManager;
import com.distributed_task_framework.utils.DistributedTaskCacheSettings;
import com.distributed_task_framework.utils.MetricHelper;
import com.distributed_task_framework.utils.TaskGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class SagaManagerImpl implements SagaManager {
    private static final String SAGA_MANAGER_CACHE = "sagaManagerCache";
    public static final TaskDef<Void> INTERNAL_SAGA_MANAGER_TASK_DEF = TaskDef.privateTaskDef("INTERNAL_SAGA_MANAGER_TASK");

    DistributedTaskService distributedTaskService;
    SagaRepository sagaRepository;
    DlsSagaContextRepository dlsSagaContextRepository;
    DistributedTaskCache<Optional<Saga>> sagaEntityDistributedTaskCache;
    SagaHelper sagaHelper;
    SagaMapper sagaMapper;
    PlatformTransactionManager transactionManager;
    SagaCommonSettings sagaCommonSettings;
    MeterRegistry meterRegistry;
    MetricHelper metricHelper;
    Clock clock;
    AtomicBoolean handleDeprecatedSagasEnabled;
    AtomicBoolean isHandleDeprecatedSagasEnabled;

    public SagaManagerImpl(DistributedTaskService distributedTaskService,
                           SagaRepository sagaRepository,
                           DlsSagaContextRepository dlsSagaContextRepository,
                           DistributedTaskCacheManager distributedTaskCacheManager,
                           SagaHelper sagaHelper,
                           SagaMapper sagaMapper,
                           PlatformTransactionManager transactionManager,
                           SagaCommonSettings sagaCommonSettings,
                           MeterRegistry meterRegistry,
                           MetricHelper metricHelper,
                           Clock clock) {
        this.distributedTaskService = distributedTaskService;
        this.sagaRepository = sagaRepository;
        this.dlsSagaContextRepository = dlsSagaContextRepository;
        this.sagaEntityDistributedTaskCache = distributedTaskCacheManager.getOrCreateCache(
            SAGA_MANAGER_CACHE,
            DistributedTaskCacheSettings.builder()
                .expireAfterWrite(sagaCommonSettings.getCacheExpiration())
                .build()
        );
        this.sagaHelper = sagaHelper;
        this.clock = clock;
        this.sagaMapper = sagaMapper;
        this.transactionManager = transactionManager;
        this.sagaCommonSettings = sagaCommonSettings;
        this.meterRegistry = meterRegistry;
        this.metricHelper = metricHelper;
        this.handleDeprecatedSagasEnabled = new AtomicBoolean(true);
        this.isHandleDeprecatedSagasEnabled = new AtomicBoolean(true);
    }

    @PostConstruct
    public void init() throws Exception {
        var taskSettings = TaskSettings.builder()
            .retry(Retry.builder()
                .retryMode(RetryMode.OFF)
                .build()
            )
            .cron(sagaCommonSettings.getDeprecatedSagaScanFixedDelay().toString())
            .maxParallelInCluster(1)
            .dltEnabled(false)
            .executionGuarantees(TaskSettings.ExecutionGuarantees.AT_LEAST_ONCE)
            .build();

        distributedTaskService.registerTask(
            TaskGenerator.defineTask(
                INTERNAL_SAGA_MANAGER_TASK_DEF,
                ctx -> handleDeprecatedSagas()
            ),
            taskSettings
        );
        //todo: may be should be scheduled async?
        distributedTaskService.schedule(INTERNAL_SAGA_MANAGER_TASK_DEF, ExecutionContext.empty());
    }

    @PreDestroy
    public void shutdown() {
        distributedTaskService.unregisterTask(INTERNAL_SAGA_MANAGER_TASK_DEF);
    }

    private void handleDeprecatedSagas() {
        if (handleDeprecatedSagasEnabled.get()) {
            handleCompletedSagas();
            handleExpiredSagas();
            isHandleDeprecatedSagasEnabled.set(true);
        } else {
            isHandleDeprecatedSagasEnabled.set(false);
        }
    }

    @VisibleForTesting
    void enableHandleDeprecatedSagas(boolean enabled) {
        handleDeprecatedSagasEnabled.set(enabled);
    }

    @VisibleForTesting
    boolean isHandleDeprecatedSagasEnabled() {
        return isHandleDeprecatedSagasEnabled.get();
    }

    private void handleCompletedSagas() {
        var removedCompletedSagaContexts = sagaRepository.removeCompleted();
        if (removedCompletedSagaContexts.isEmpty()) {
            return;
        }

        log.info("handleCompletedSagas(): removedCompletedSagaContexts=[{}]", removedCompletedSagaContexts);
        removedCompletedSagaContexts.forEach(shortSagaEntity ->
            getRemoveCompletedSagaCounter(shortSagaEntity).increment()
        );
    }

    private void handleExpiredSagas() {
        var transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.executeWithoutResult(status -> {
            var expiredSagaContextEntities = sagaRepository.findExpired(
                sagaCommonSettings.getExpiredSagaBatchSizeToRemove()
            );
            if (expiredSagaContextEntities.isEmpty()) {
                return;
            }

            var expiredSagaIds = expiredSagaContextEntities.stream()
                .map(SagaEntity::getSagaId)
                .toList();
            log.info("handleExpiredSagas(): expiredSagaIds=[{}]", expiredSagaIds);
            forceShutdown(expiredSagaContextEntities, true);
            expiredSagaContextEntities.forEach(sagaEntity -> getExpiredShutdownSagaCounter(sagaEntity).increment());
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
            .stopOnFailedAnyRevert(sagaSettings.isStopOnFailedAnyRevert())
            .build();
        sagaRepository.saveOrUpdate(sagaEntity);
        getCreateSagaCounter(sagaEntity).increment();
    }

    @Override
    public Saga get(UUID sagaId) throws SagaNotFoundException {
        return getIfExists(sagaId).orElseThrow(sagaNotFoundException(sagaId));
    }

    // cached because client can poll this method
    @Override
    public void checkExistence(UUID sagaId) throws SagaNotFoundException {
        if (getCachedSagaEntityById(sagaId).isEmpty()) {
            throw sagaNotFoundException(sagaId).get();
        }
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

        boolean isNotCompleted = sagaResultEntity.getCompletedDateUtc() == null;
        if (isNotCompleted) {
            return Optional.empty();
        }

        if (StringUtils.isNotBlank(sagaResultEntity.getExceptionType())) {
            throw sagaHelper.buildExecutionException(sagaResultEntity.getExceptionType(), sagaResultEntity.getResult());
        }

        return sagaResultEntity.getResult() != null ?
            sagaHelper.buildObject(sagaResultEntity.getResult(), resultType) :
            Optional.empty();
    }

    @Override
    public Optional<Saga> getIfExists(UUID sagaId) {
        return sagaRepository.findShortById(sagaId)
            .map(sagaMapper::toModel);
    }

    // cached because client can poll this method
    @Override
    public boolean isCompleted(UUID sagaId) throws SagaNotFoundException {
        return getCachedSagaEntityById(sagaId)
            .map(sagaEntity -> sagaEntity.getCompletedDateUtc() != null)
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
        var now = LocalDateTime.now(clock);
        updateUnderLock(
            sagaId,
            sagaContextEntity -> sagaContextEntity.toBuilder()
                .completedDateUtc(now)
                .expirationDateUtc(now.plusSeconds(sagaContextEntity.getAvailableAfterCompletionTimeoutSec()))
                .build(),
            sagaEntity -> {
                getCompletedSagaCounter(sagaEntity).increment();
                getCompletedTimer(sagaEntity).record(
                    Duration.between(sagaEntity.getCreatedDateUtc(), sagaEntity.getCompletedDateUtc())
                );
            },
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
            sagaEntity -> getCompletedOkSagaCounter(sagaEntity).increment(),
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
            sagaEntity -> getCompletedFailedSagaCounter(sagaEntity).increment(),
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
            sagaEntity -> {
                getCanceledSagaCounter(sagaEntity).increment();
            },
            true
        );
    }

    @Override
    public void forceShutdown(UUID sagaId) {
        log.info("forceShutdown(): shutdown sagaId=[{}]", sagaId);
        sagaRepository.findById(sagaId)
            .ifPresentOrElse(
                //don't move shutdown sagas to DLS because it is explicit action
                sagaEntity -> {
                    forceShutdown(List.of(sagaEntity), false);
                    getShutdownByManualSagaCounter(sagaEntity).increment();
                    getCompletedTimer(sagaEntity).record(
                        Duration.between(sagaEntity.getCreatedDateUtc(), LocalDateTime.now(clock))
                    );
                },
                () -> {
                    throw sagaNotFoundException(sagaId).get();
                }
            );
    }

    private Optional<Saga> getCachedSagaEntityById(UUID sagaId) {
        return sagaEntityDistributedTaskCache.get(
            sagaId.toString(),
            () -> sagaRepository.findShortById(sagaId).map(sagaMapper::toModel)
        );
    }

    private void updateUnderLock(UUID sagaId,
                                 Function<SagaEntity, SagaEntity> updateAction,
                                 boolean strictMode) throws SagaNotFoundException {
        updateUnderLock(sagaId, updateAction, null, strictMode);
    }

    // NOTE: We use pessimistic update in order to protect from parallel changes, and
    // we will prepare to support parallel-saga mode. Parallel changes may be from client code,
    // for example cancellation and task any code from task.
    // Or for instance, in that mode SagaPipeline will be merged with existed in db before saving.
    private void updateUnderLock(UUID sagaId,
                                 Function<SagaEntity, SagaEntity> updateAction,
                                 Consumer<SagaEntity> ofSuccessConsumer,
                                 boolean strictMode) throws SagaNotFoundException {
        var transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        transactionTemplate.executeWithoutResult(status -> {
                sagaRepository.findByIdIfExistsForUpdate(sagaId)
                    .map(updateAction)
                    .ifPresentOrElse(
                        sagaEntity -> {
                            final var sagaEntityFinal = sagaRepository.save(sagaEntity);
                            Optional.ofNullable(ofSuccessConsumer)
                                .ifPresent(consumer -> consumer.accept(sagaEntityFinal));
                        },
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

    private Counter getCreateSagaCounter(SagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "create"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Counter getCompletedSagaCounter(SagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "completed"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Counter getCompletedOkSagaCounter(SagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "complete-ok-with-result"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Counter getCompletedFailedSagaCounter(SagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "complete-failed-with-result"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Counter getCanceledSagaCounter(SagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "cancel"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Counter getRemoveCompletedSagaCounter(ShortSagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "removed"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Counter getShutdownByManualSagaCounter(SagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "manual-shutdown"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Counter getExpiredShutdownSagaCounter(SagaEntity sagaEntity) {
        return metricHelper.counter(
            List.of("saga", "action", "expired-shutdown"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }

    private Timer getCompletedTimer(SagaEntity sagaEntity) {
        return metricHelper.timer(
            List.of("saga", "timer", "completed"),
            List.of(Tag.of("saga_name", sagaEntity.getName()))
        );
    }
}
