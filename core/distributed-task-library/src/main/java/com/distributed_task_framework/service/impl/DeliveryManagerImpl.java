package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.mapper.CommandMapper;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientException;
import reactor.netty.http.client.HttpClient;
import com.distributed_task_framework.controller.dto.CommandListDto;
import com.distributed_task_framework.persistence.entity.RemoteCommandEntity;
import com.distributed_task_framework.persistence.entity.RemoteTaskWorkerEntity;
import com.distributed_task_framework.persistence.repository.DlcRepository;
import com.distributed_task_framework.persistence.repository.RemoteCommandRepository;
import com.distributed_task_framework.persistence.repository.RemoteTaskWorkerRepository;
import com.distributed_task_framework.service.impl.remote_commands.RemoteCommand;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.DeliveryManager;
import com.distributed_task_framework.settings.CommonSettings;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.distributed_task_framework.service.impl.remote_commands.RemoteCommand.NAME_OF_PART;

/**
 * TODO: methods to recovery from DLC (-)
 */
@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
public class DeliveryManagerImpl implements DeliveryManager {
    CommonSettings.DeliveryManagerSettings commandDeliverySettings;
    RemoteTaskWorkerRepository remoteTaskWorkerRepository;
    RemoteCommandRepository remoteCommandRepository;
    DlcRepository dlcRepository;
    ClusterProvider clusterProvider;
    ScheduledExecutorService watchdogExecutorService;
    ExecutorService deliveryExecutorService;
    Map<String, Future<Void>> deliverTaskByAppName;
    HttpClient httpClient;
    CommandMapper commandMapper;
    TaskSerializer taskSerializer;
    PlatformTransactionManager transactionManager;
    Clock clock;

    public DeliveryManagerImpl(CommonSettings commonSettings,
                               RemoteTaskWorkerRepository remoteTaskWorkerRepository,
                               RemoteCommandRepository remoteCommandRepository,
                               DlcRepository dlcRepository,
                               ClusterProvider clusterProvider,
                               CommandMapper commandMapper,
                               TaskSerializer taskSerializer,
                               PlatformTransactionManager transactionManager,
                               Clock clock) {
        this.commandDeliverySettings = commonSettings.getDeliveryManagerSettings();
        this.remoteTaskWorkerRepository = remoteTaskWorkerRepository;
        this.remoteCommandRepository = remoteCommandRepository;
        this.dlcRepository = dlcRepository;
        this.clusterProvider = clusterProvider;
        this.commandMapper = commandMapper;
        this.taskSerializer = taskSerializer;
        this.transactionManager = transactionManager;
        this.clock = clock;
        this.deliverTaskByAppName = Maps.newHashMap();
        this.httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) commandDeliverySettings.getConnectionTimeout().toMillis())
                .responseTimeout(commandDeliverySettings.getResponseTimeout())
                .doOnConnected(conn ->
                        conn.addHandlerLast(new ReadTimeoutHandler(
                                commandDeliverySettings.getReadTimeout().toMillis(),
                                TimeUnit.MILLISECONDS
                        ))
                                .addHandlerLast(new WriteTimeoutHandler(
                                        commandDeliverySettings.getWriteTimeout().toMillis(),
                                        TimeUnit.MILLISECONDS
                                ))
                );
        this.watchdogExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("output-watch-%d")
                .setUncaughtExceptionHandler((t, e) -> {
                    log.error("scheduleWatchdog(): error when watch by schedule table", e);
                    ReflectionUtils.rethrowRuntimeException(e);
                })
                .build()
        );
        this.deliveryExecutorService = Executors.newFixedThreadPool(
                commonSettings.getDeliveryManagerSettings().getRemoteApps().getAppToUrl().size(),
                new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("delivery-%d")
                        .setUncaughtExceptionHandler((t, e) -> {
                            log.error("delivery(): error when run delivery", e);
                            ReflectionUtils.rethrowRuntimeException(e);
                        })
                        .build()
        );
    }

    @PostConstruct
    public void init() {
        watchdogExecutorService.scheduleWithFixedDelay(
                ExecutorUtils.wrapRepeatableRunnable(this::watchdog),
                commandDeliverySettings.getWatchdogInitialDelayMs(),
                commandDeliverySettings.getWatchdogFixedDelayMs(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * @noinspection ResultOfMethodCallIgnored
     */
    @PreDestroy
    public void shutdown() throws InterruptedException {
        log.info("shutdown(): shutdown started");
        watchdogExecutorService.shutdownNow();
        deliveryExecutorService.shutdownNow();
        watchdogExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        deliveryExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        log.info("shutdown(): shutdown completed");
    }

    @VisibleForTesting
    void watchdog() {
        commandDeliverySettings.getRemoteApps()
                .getAppToUrl()
                .keySet()
                .forEach(this::watchdog);
    }

    private void watchdog(String appName) {
        Optional<RemoteTaskWorkerEntity> activeDeliveryOpt = remoteTaskWorkerRepository.findByAppName(appName);
        Future<?> deliveryLoopFuture = deliverTaskByAppName.get(appName);
        if (activeDeliveryOpt.isPresent()) {
            RemoteTaskWorkerEntity activeDelivery = activeDeliveryOpt.get();
            if (clusterProvider.nodeId().equals(activeDelivery.getNodeStateId())) {
                if (deliveryLoopFuture == null) {
                    log.warn("watchdog(): delivery manager=[{}] for appName=[{}] hasn't been started right after was assigned, starting...",
                            clusterProvider.nodeId(), appName);
                    startDeliveryLoop(appName);
                } else if (deliveryLoopFuture.isDone()) {
                    log.error("watchdog(): delivery manager=[{}] for appName=[{}] has been completed abnormally, restart it",
                            clusterProvider.nodeId(), appName);
                    startDeliveryLoop(appName);
                }
                //do nothing it is ok.
                return;
            }
            if (deliveryLoopFuture != null) {
                log.error("watchdog(): delivery manager for appName=[{}] conflict detected: nodeId=[{}], concurrentPlannerNodeId=[{}]",
                        appName, clusterProvider.nodeId(), activeDelivery.getNodeStateId());
                deliveryLoopFuture.cancel(true);
                deliverTaskByAppName.remove(appName);
            }
            return;
        }
        log.info("watchdog(): can't detect active delivery manager for appName=[{}], try to become it", appName);
        try {
            remoteTaskWorkerRepository.save(RemoteTaskWorkerEntity.builder()
                    .appName(appName)
                    .nodeStateId(clusterProvider.nodeId())
                    .build()
            );
        } catch (Exception exception) {
            log.info("watchdog(): nodeId=[{}] can't become an active delivery manager for appName=[{}]", clusterProvider.nodeId(), appName, exception);
            return;
        }
        if (deliveryLoopFuture == null) {
            startDeliveryLoop(appName);
            log.info("watchdog(): nodeId=[{}] is an active delivery manager for appName=[{}]", clusterProvider.nodeId(), appName);
        } else {
            log.warn("watchdog(): nodeId=[{}] is an active delivery manager and has already started deliveryLoop", clusterProvider.nodeId());
        }
    }

    /**
     * @noinspection unchecked
     */
    private void startDeliveryLoop(String appName) {
        Future<Void> future = (Future<Void>) deliveryExecutorService.submit(ExecutorUtils.wrapRunnable(() -> deliveryLoop(appName)));
        deliverTaskByAppName.put(appName, future);
    }

    @SneakyThrows
    @VisibleForTesting
    void deliveryLoop(String appName) {
        log.info("deliveryLoop(): has been started for appName=[{}]", appName);
        var baseUri = commandDeliverySettings.getRemoteApps().getAppToUrl().get(appName).toString();
        WebClient webClient = WebClient.builder()
                .baseUrl(baseUri)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, org.springframework.http.MediaType.MULTIPART_FORM_DATA.toString())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();

        while (!Thread.currentThread().isInterrupted()) {
            int taskNumber = deliveryCommand(appName, webClient);
            log.debug("deliveryLoop(): delivery tasks=[{}] for appName=[{}]", taskNumber, appName);
            try {
                sleep(taskNumber);
            } catch (InterruptedException e) {
                log.info("planningLoop(): has been interrupted.");
                return;
            }
        }
        log.info("deliveryLoop(): has been stopped for appName=[{}]", appName);
    }

    private int deliveryCommand(String appName, WebClient webClient) throws IOException {
        Collection<RemoteCommandEntity> commandsToSend = remoteCommandRepository.findCommandsToSend(
                appName,
                LocalDateTime.now(clock),
                commandDeliverySettings.getBatchSize()
        );
        int commandSize = commandsToSend.size();
        if (commandsToSend.isEmpty()) {
            return 0;
        }
        CommandListDto commandListDto = commandMapper.mapToList(commandsToSend);
        byte[] message = taskSerializer.writeValue(commandListDto);

        MultipartBodyBuilder multipartBodyBuilder = new MultipartBodyBuilder();
        multipartBodyBuilder.part(NAME_OF_PART, message);
        MultiValueMap<String, HttpEntity<?>> multipartBody = multipartBodyBuilder.build();

        try {
            ResponseEntity<Void> responseEntity = webClient.post()
                    .uri(RemoteCommand.DEFAULT_COMMAND_PATH)
                    .body(BodyInserters.fromMultipartData(multipartBody))
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            if (responseEntity == null) {
                log.warn("Can't delivery commands=[{}] for appName=[{}], responseEntity is null", commandSize, appName);
                return commandSize;
            }
            if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                log.warn(
                        "Can't delivery commands=[{}] for appName=[{}], responce code=[{}]",
                        commandSize,
                        appName,
                        responseEntity.getStatusCode()
                );
                return commandSize;
            }
        } catch (WebClientException exception) {
            log.warn("Can't delivery commands=[{}] for appName=[{}]", commandSize, appName, exception);
            applyRetryPolicy(appName, commandsToSend);
            return commandSize;
        }
        remoteCommandRepository.deleteAll(commandsToSend);
        return commandSize;
    }

    private void applyRetryPolicy(String appName, Collection<RemoteCommandEntity> failedCommandsToSend) {
        List<RemoteCommandEntity> toUpdate = Lists.newArrayList();
        List<RemoteCommandEntity> toDlc = Lists.newArrayList();
        for (RemoteCommandEntity remoteCommandEntity : failedCommandsToSend) {
            int currentFailures = remoteCommandEntity.getFailures() + 1;
            remoteCommandEntity = remoteCommandEntity.toBuilder()
                    .failures(currentFailures)
                    .build();
            Optional<LocalDateTime> nextRetryDateTime = commandDeliverySettings.getRetry().nextRetry(currentFailures, clock);
            if (nextRetryDateTime.isPresent()) {
                remoteCommandEntity = remoteCommandEntity.toBuilder()
                        .sendDateUtc(nextRetryDateTime.get())
                        .build();
                toUpdate.add(remoteCommandEntity);
            } else {
                toDlc.add(remoteCommandEntity);
            }
        }
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            remoteCommandRepository.saveAll(toUpdate);
            if (!toDlc.isEmpty()) {
                remoteCommandRepository.deleteAll(toDlc);
                dlcRepository.saveAll(commandMapper.mapToDlcList(toDlc));
                log.warn(
                        "applyRetryPolicy(): appName=[{}] sent [{}] commands to DLC: [{}]",
                        appName,
                        toDlc.size(),
                        toDlc.stream().map(RemoteCommandEntity::getAction).collect(Collectors.toSet())
                );
            }
        });
    }

    @SuppressWarnings("DataFlowIssue")
    private void sleep(int taskNumber) throws InterruptedException {
        Integer maxInConfig = commandDeliverySettings.getManageDelay().span().upperEndpoint();
        taskNumber = Math.min(taskNumber, maxInConfig);
        Integer delayMs = commandDeliverySettings.getManageDelay().get(taskNumber);
        TimeUnit.MILLISECONDS.sleep(delayMs);
    }
}
