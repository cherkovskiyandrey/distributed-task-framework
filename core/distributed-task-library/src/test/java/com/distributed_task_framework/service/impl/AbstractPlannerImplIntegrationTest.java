package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.entity.PlannerEntity;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.persistence.repository.PlannerRepository;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.service.internal.DistributedTaskMetricHelper;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.distributed_task_framework.settings.CommonSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;

@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class AbstractPlannerImplIntegrationTest extends BaseSpringIntegrationTest {
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    PlannerRepository plannerRepository;
    @Autowired
    NodeStateRepository nodeStateRepository;
    @Autowired
    PlatformTransactionManager transactionManager;
    @Autowired
    Clock clock;
    @Autowired
    DistributedTaskMetricHelper distributedTaskMetricHelper;
    AbstractPlannerImpl plannerService;
    ExecutorService executorService;

    @BeforeEach
    public void init() {
        super.init();
        executorService = Executors.newSingleThreadExecutor();
        plannerService = Mockito.spy(new DummyPlanner(
                commonSettings,
                plannerRepository,
                transactionManager,
                clusterProvider,
            distributedTaskMetricHelper
        ));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @SneakyThrows
    @AfterEach
    void destroy() {
        plannerService.shutdown();
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    void shouldRunPlannerLoop() {
        //when
        AtomicInteger flag = mockPlanningLoopInvocation();
        waitForNodeIsRegistered();

        //do
        plannerService.watchdog();

        //verify
        waitFor(() -> flag.get() > 0);
        assertThat(plannerRepository.findByGroupName(DummyPlanner.GROUP_NAME)).isPresent()
                .get()
                .matches(plannerEntity -> plannerEntity.getNodeStateId().equals(clusterProvider.nodeId()));
    }

    @Test
    void shouldNotRunPlannerLoopWhenOtherServiceIsLocked() {
        //when
        UUID foreignNodeId = UUID.randomUUID();
        nodeStateRepository.save(NodeStateEntity.builder()
                .lastUpdateDateUtc(LocalDateTime.now(clock).plusHours(1))
                .node(foreignNodeId)
                .build()
        );
        plannerRepository.save(PlannerEntity.builder()
                .groupName(DummyPlanner.GROUP_NAME)
                .nodeStateId(foreignNodeId)
                .build());
        waitForNodeIsRegistered();

        //do
        plannerService.watchdog();

        //verify
        assertThat(plannerRepository.findByGroupName(DummyPlanner.GROUP_NAME)).isPresent()
                .get()
                .matches(plannerEntity -> plannerEntity.getNodeStateId().equals(foreignNodeId));
    }

    @Test
    void shouldRestartPlanningLoopWhenLockedButNotStarted() {
        //when
        AtomicInteger flag = mockPlanningLoopInvocation();
        waitForNodeIsRegistered();
        plannerRepository.save(PlannerEntity.builder()
                .groupName(DummyPlanner.GROUP_NAME)
                .nodeStateId(clusterProvider.nodeId())
                .build());

        //do
        plannerService.watchdog();

        //verify
        waitFor(() -> flag.get() > 0);
        assertThat(plannerRepository.findByGroupName(DummyPlanner.GROUP_NAME)).isPresent()
                .get()
                .matches(plannerEntity -> plannerEntity.getNodeStateId().equals(clusterProvider.nodeId()));
    }

    @Test
    void shouldStopPlanningLoopWhenSplitBrain() {
        //when
        DeliveryLoopSignals deliveryLoopSignals = mockInfinityDeliveryLoopInvocation();
        waitForNodeIsRegistered();
        plannerService.watchdog();
        waitFor(() -> deliveryLoopSignals.getStartSignal().get() > 0);

        plannerRepository.deleteAll();
        UUID foreignNodeId = UUID.randomUUID();
        nodeStateRepository.save(NodeStateEntity.builder()
                .lastUpdateDateUtc(LocalDateTime.now(clock).plusHours(1))
                .node(foreignNodeId)
                .build()
        );
        plannerRepository.save(PlannerEntity.builder()
                .groupName(DummyPlanner.GROUP_NAME)
                .nodeStateId(foreignNodeId)
                .build());

        //do
        plannerService.watchdog();

        //verify
        waitFor(() -> deliveryLoopSignals.getStopSignal().get() > 0);
    }

    @Test
    void shouldStopPlanningLoopWhenIsNotActiveAndRun() {
        //when
        DeliveryLoopSignals deliveryLoopSignals = mockInfinityDeliveryLoopInvocation();
        waitForNodeIsRegistered();
        plannerService.watchdog();
        waitFor(() -> deliveryLoopSignals.getStartSignal().get() > 0);

        ((DummyPlanner)plannerService).setActive(false);

        //do
        plannerService.watchdog();

        //verify
        waitFor(() -> deliveryLoopSignals.getStopSignal().get() > 0);
    }

    private AtomicInteger mockPlanningLoopInvocation() {
        AtomicInteger flag = new AtomicInteger(0);
        doAnswer(invocation -> {
            flag.incrementAndGet();
            return null;
        }).when(plannerService).planningLoop();
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
        }).when(plannerService).planningLoop();
        return deliveryLoopSignals;
    }

    private static class DummyPlanner extends AbstractPlannerImpl {
        private static final String GROUP_NAME = "dummy group";
        private static final String NAME = "dummy";

        @Setter
        @Getter
        private volatile boolean active;

        public DummyPlanner(CommonSettings commonSettings,
                            PlannerRepository plannerRepository,
                            PlatformTransactionManager transactionManager,
                            ClusterProvider clusterProvider,
                            DistributedTaskMetricHelper distributedTaskMetricHelper) {
            super(commonSettings, plannerRepository, transactionManager, clusterProvider, distributedTaskMetricHelper);
            this.active = true;
        }

        @Override
        protected String name() {
            return NAME;
        }

        @Override
        protected String shortName() {
            return NAME;
        }

        @Override
        protected String groupName() {
            return GROUP_NAME;
        }

        @Override
        protected boolean hasToBeActive() {
            return active;
        }

        @Override
        int processInLoop() {
            return 0;
        }
    }
}
