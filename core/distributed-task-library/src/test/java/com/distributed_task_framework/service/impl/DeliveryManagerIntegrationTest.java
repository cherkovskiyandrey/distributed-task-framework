package com.distributed_task_framework.service.impl;


import com.distributed_task_framework.mapper.CommandMapper;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.controller.dto.CommandListDto;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.entity.RemoteCommandEntity;
import com.distributed_task_framework.persistence.entity.RemoteTaskWorkerEntity;
import com.distributed_task_framework.service.TaskSerializer;
import com.distributed_task_framework.utils.ExecutorUtils;
import com.distributed_task_framework.service.impl.remote_commands.ScheduleCommand;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.CommonSettings;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.fileupload.MultipartStream;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.http.entity.ContentType;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.PlatformTransactionManager;

import jakarta.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
class DeliveryManagerIntegrationTest extends BaseSpringIntegrationTest {
    private static final byte[] EMBEDDED_COMMAND = "very important command".getBytes(StandardCharsets.UTF_8);
    private static MockWebServer mockWebServer;
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    CommandMapper commandMapper;
    @Autowired
    TaskSerializer taskSerializer;
    @Autowired
    PlatformTransactionManager transactionManager;
    ExecutorService executorService;
    DeliveryManagerImpl deliveryManager;

    @SneakyThrows
    @BeforeEach
    public void init() {
        super.init();
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        executorService = Executors.newSingleThreadExecutor();

        HttpUrl url = mockWebServer.url("/");
        CommonSettings overriddenCommonSettings = this.commonSettings.toBuilder()
                .deliveryManagerSettings(this.commonSettings.getDeliveryManagerSettings().toBuilder()
                        .remoteApps(CommonSettings.RemoteApps.builder()
                                .appToUrl(
                                        Map.of(
                                                "foreign-test-app", url.url()
                                        )
                                )
                                .build())
                        .build())
                .build();

        deliveryManager = Mockito.spy(new DeliveryManagerImpl(
                overriddenCommonSettings,
                remoteTaskWorkerRepository,
                remoteCommandRepository,
                dlcRepository,
                clusterProvider,
                commandMapper,
                taskSerializer,
                transactionManager,
                clock
        ));
    }

    @SneakyThrows
    @AfterEach
    public void destroy() {
        deliveryManager.shutdown();
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        mockWebServer.shutdown();
    }

    @Test
    void shouldRunDeliveryLoop() {
        //when
        AtomicInteger flag = mockDeliveryLoopInvocation();
        waitForNodeIsRegistered();

        //do
        deliveryManager.watchdog();

        //verify
        waitFor(() -> flag.get() > 0);
        Assertions.assertThat(remoteTaskWorkerRepository.findByAppName("foreign-test-app")).isPresent()
                .get()
                .matches(entity -> clusterProvider.nodeId().equals(entity.getNodeStateId()));
    }

    @Test
    void shouldNotRunDeliveryLoopWhenOtherServiceIsLocked() {
        //when
        UUID foreignNodeId = UUID.randomUUID();
        nodeStateRepository.save(NodeStateEntity.builder()
                .lastUpdateDateUtc(LocalDateTime.now(clock).plusHours(1).withNano(0))
                .node(foreignNodeId)
                .build()
        );
        remoteTaskWorkerRepository.save(RemoteTaskWorkerEntity.builder()
                .appName("foreign-test-app")
                .nodeStateId(foreignNodeId)
                .build()
        );
        waitForNodeIsRegistered();

        //do
        deliveryManager.watchdog();

        //verify
        Assertions.assertThat(remoteTaskWorkerRepository.findByAppName("foreign-test-app")).isPresent()
                .get()
                .matches(entity -> entity.getNodeStateId().equals(foreignNodeId));
    }

    @Test
    void shouldRestartDeliveryLoopWhenLockedButNotStarted() {
        //when
        AtomicInteger flag = mockDeliveryLoopInvocation();
        waitForNodeIsRegistered();
        remoteTaskWorkerRepository.save(RemoteTaskWorkerEntity.builder()
                .appName("foreign-test-app")
                .nodeStateId(clusterProvider.nodeId())
                .build()
        );

        //do
        deliveryManager.watchdog();

        //verify
        waitFor(() -> flag.get() > 0);
        Assertions.assertThat(remoteTaskWorkerRepository.findByAppName("foreign-test-app")).isPresent()
                .get()
                .matches(entity -> clusterProvider.nodeId().equals(entity.getNodeStateId()));
    }

    @Test
    void shouldStopDeliveryLoopWhenSplitBrain() {
        //when
        DeliveryLoopSignals deliveryLoopSignals = mockInfinityDeliveryLoopInvocation();
        waitForNodeIsRegistered();
        deliveryManager.watchdog();
        waitFor(() -> deliveryLoopSignals.getStartSignal().get() > 0);

        remoteTaskWorkerRepository.deleteAll();
        UUID foreignNodeId = UUID.randomUUID();
        nodeStateRepository.save(NodeStateEntity.builder()
                .lastUpdateDateUtc(LocalDateTime.now(clock).plusHours(1))
                .node(foreignNodeId)
                .build()
        );
        remoteTaskWorkerRepository.save(RemoteTaskWorkerEntity.builder()
                .appName("foreign-test-app")
                .nodeStateId(foreignNodeId)
                .build()
        );

        //do
        deliveryManager.watchdog();

        //verify
        waitFor(() -> deliveryLoopSignals.getStopSignal().get() > 0);
    }

    @SneakyThrows
    @Test
    void shouldSendBatchOfNewCommands() {
        //when
        Collection<RemoteCommandEntity> firstBatch = createCommands(100, "foreign-test-app");
        Collection<RemoteCommandEntity> secondBatch = createCommands(100, "foreign-test-app");

        mockWebServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value()));
        mockWebServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value()));

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(() -> deliveryManager.deliveryLoop("foreign-test-app")));

        //verify
        verifyBatch(firstBatch);
        verifyBatch(secondBatch);
        waitFor(() -> Lists.newArrayList(remoteCommandRepository.findAll()).isEmpty());
    }

    @Test
    void shouldNotSendForeignCommands() {
        //when
        Collection<RemoteCommandEntity> firstBatch = createCommands(50, "foreign-test-app");
        Collection<RemoteCommandEntity> foreignBatch = createCommands(50, "unknown-foreign-test-app");

        mockWebServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value()));

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(() -> deliveryManager.deliveryLoop("foreign-test-app")));

        //verify
        verifyBatch(firstBatch);
        waitFor(() -> EqualsBuilder.reflectionEquals(
                Lists.newArrayList(remoteCommandRepository.findAll()),
                foreignBatch,
                "createdDateUtc",
                "sendDateUtc"
        ));
    }

    @SneakyThrows
    @Test
    void shouldTryToSendWhenSomeErrorsOnServerSide() {
        //when
        Collection<RemoteCommandEntity> batch = createCommands(50, "foreign-test-app");

        mockWebServer.enqueue(new MockResponse().setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR.value()));
        mockWebServer.enqueue(new MockResponse().setResponseCode(HttpStatus.PERMANENT_REDIRECT.value()));
        mockWebServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value()));

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(() -> deliveryManager.deliveryLoop("foreign-test-app")));

        //verify
        mockWebServer.takeRequest(10, TimeUnit.SECONDS);
        mockWebServer.takeRequest(10, TimeUnit.SECONDS);
        verifyBatch(batch);
        waitFor(() -> Lists.newArrayList(remoteCommandRepository.findAll()).isEmpty());
    }

    @Test
    void shouldMoveCommandsToDlcWhenAttempsExceed() {
        //when
        Collection<RemoteCommandEntity> batch = createCommands(50, "foreign-test-app");

        IntStream.range(0, 5).forEach(i -> {
            mockWebServer.enqueue(new MockResponse().setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR.value()));
        });

        //do
        executorService.submit(ExecutorUtils.wrapRunnable(() -> deliveryManager.deliveryLoop("foreign-test-app")));

        //verify
        waitFor(() -> Lists.newArrayList(remoteCommandRepository.findAll()).isEmpty());
        waitFor(() -> EqualsBuilder.reflectionEquals(
                Lists.newArrayList(dlcRepository.findAll()),
                commandMapper.mapToDlcList(batch),
                "createdDateUtc"
        ));
    }


    private Collection<RemoteCommandEntity> createCommands(int number, String appName) {
        List<RemoteCommandEntity> commandBatch = IntStream.range(0, number).mapToObj(i ->
                        RemoteCommandEntity.builder()
                                .appName(appName)
                                .createdDateUtc(LocalDateTime.now(clock).withNano(0))
                                .sendDateUtc(LocalDateTime.now(clock).withNano(0))
                                .action(ScheduleCommand.NAME)
                                .taskName("some-task-name")
                                .body(EMBEDDED_COMMAND)
                                .build())
                .collect(Collectors.toList());
        return Lists.newArrayList(remoteCommandRepository.saveAll(commandBatch));
    }

    @SneakyThrows
    private void verifyBatch(Collection<RemoteCommandEntity> batch) {
        RecordedRequest fireBatchRequest = mockWebServer.takeRequest(10, TimeUnit.SECONDS);
        CommandListDto realCommandListDto = readBody(fireBatchRequest);
        CommandListDto expectedReceivedCommands = commandMapper.mapToList(batch);
        assertThat(realCommandListDto).usingRecursiveComparison()
                .ignoringFieldsMatchingRegexes(".*DateUtc.*")
                .isEqualTo(expectedReceivedCommands);
    }

    @SneakyThrows
    private CommandListDto readBody(@Nullable RecordedRequest batchRequest) {
        assertThat(batchRequest).isNotNull();
        String contentHeader = batchRequest.getHeader(HttpHeaders.CONTENT_TYPE);
        assertThat(contentHeader).isNotBlank();
        ContentType contentType = ContentType.parse(contentHeader);
        assertThat(contentType.getParameter("boundary")).isNotBlank();

        MultipartStream multipartStream = new MultipartStream(
                new ByteArrayInputStream(batchRequest.getBody().readByteArray()),
                contentType.getParameter("boundary").getBytes(),
                1024,
                null);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        boolean nextPart = multipartStream.skipPreamble();
        while (nextPart) {
            multipartStream.readHeaders(); //skip headers
            multipartStream.readBodyData(output);
            nextPart = multipartStream.readBoundary();
        }

        byte[] receivedBody = output.toByteArray();
        return taskSerializer.readValue(receivedBody, CommandListDto.class);
    }


    private void waitForNodeIsRegistered() {
        waitFor(() -> nodeStateRepository.findById(clusterProvider.nodeId()).isPresent());
    }

    private AtomicInteger mockDeliveryLoopInvocation() {
        AtomicInteger flag = new AtomicInteger(0);
        doAnswer(invocation -> {
            flag.incrementAndGet();
            return null;
        }).when(deliveryManager).deliveryLoop(eq("foreign-test-app"));
        return flag;
    }

    private DeliveryLoopSignals mockInfinityDeliveryLoopInvocation() {
        DeliveryLoopSignals deliveryLoopSignals = DeliveryLoopSignals.builder()
                .startSignal(new AtomicInteger(0))
                .stopSignal(new AtomicInteger(0))
                .build();
        doAnswer(invocation -> {
            try {
                deliveryLoopSignals.getStartSignal().incrementAndGet();
                while (!Thread.currentThread().isInterrupted()) {
                    TimeUnit.SECONDS.sleep(1);
                }
            } finally {
                deliveryLoopSignals.getStopSignal().incrementAndGet();
            }
            return null;
        }).when(deliveryManager).deliveryLoop(eq("foreign-test-app"));
        return deliveryLoopSignals;
    }
}
