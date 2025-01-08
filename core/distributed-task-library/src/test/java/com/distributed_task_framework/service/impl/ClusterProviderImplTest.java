package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.NodeLoading;
import com.distributed_task_framework.BaseSpringIntegrationTest;
import com.distributed_task_framework.persistence.entity.CapabilityEntity;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.entity.RegisteredTaskEntity;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.service.internal.CapabilityRegister;
import com.distributed_task_framework.service.internal.CapabilityRegisterProvider;
import com.distributed_task_framework.service.internal.WorkerManager;
import com.google.common.collect.Lists;
import com.sun.management.OperatingSystemMXBean;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.distributed_task_framework.service.impl.ClusterProviderImpl.CPU_LOADING_UNDEFINED;
import static com.distributed_task_framework.service.impl.ClusterProviderImpl.MIN_CPU_LOADING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@FieldDefaults(level = AccessLevel.PRIVATE)
class ClusterProviderImplTest extends BaseSpringIntegrationTest {
    //turn off workers
    @MockBean
    WorkerManager workerManager;
    @Autowired
    NodeStateRepository nodeStateRepository;
    @Autowired
    ClusterProviderImpl clusterProvider;
    @SpyBean
    CapabilityRegisterProvider capabilityRegisterProvider;
    @Autowired
    PlatformTransactionManager transactionManager;
    @MockBean
    OperatingSystemMXBean operatingSystemMXBean;

    @Nested
    class NodeStateTests {

        @Test
        void shouldPeriodicallyUpdateNodeState() {
            //when
            NodeStateEntity nodeStateEntity = waitAndGet(
                () -> nodeStateRepository.findById(clusterProvider.nodeId()),
                Optional::isPresent
            ).orElseThrow();

            //verify
            waitAndGet(
                () -> nodeStateRepository.findById(clusterProvider.nodeId()),
                currentNodeStateOpt -> currentNodeStateOpt
                    .filter(currentNodeState -> Objects.equals(currentNodeState.getNode(), nodeStateEntity.getNode()))
                    .filter(currentNodeState -> currentNodeState.getLastUpdateDateUtc().isAfter(nodeStateEntity.getLastUpdateDateUtc()))
                    .isPresent()
            );
        }

        @Test
        void shouldCleanObsoleteNodes() {
            //when & do
            final UUID foreignNodeId = UUID.randomUUID();
            new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                nodeStateRepository.save(NodeStateEntity.builder()
                    .node(foreignNodeId)
                    .lastUpdateDateUtc(LocalDateTime.now(clock).minus(Duration.ofMillis(commonSettings.getRegistrySettings().getMaxInactivityIntervalMs())))
                    .build()
                );
                registeredTaskRepository.save(RegisteredTaskEntity.builder()
                    .taskName("foreign_task")
                    .nodeStateId(foreignNodeId)
                    .build()
                );
            });

            //verify
            waitAndGet(
                () -> nodeStateRepository.findById(foreignNodeId),
                Optional::isEmpty
            );
            waitAndGet(
                () -> registeredTaskRepository.findByNodeStateId(foreignNodeId),
                Collection::isEmpty
            );
        }


        @Test
        void shouldReturnClusterNodes() {
            //when
            var firstNode = createNode();
            var secondNode = createNode();
            var thirdNode = createNode();

            //verify
            Set<UUID> nodes = waitAndGet(
                () -> clusterProvider.clusterNodes(),
                n -> n.size() == 4
            );
            assertThat(nodes)
                .containsExactlyInAnyOrderElementsOf(Set.of(
                    clusterProvider.nodeId(),
                    firstNode.getNode(),
                    secondNode.getNode(),
                    thirdNode.getNode())
                );
        }
    }

    @Nested
    class NodeLoadingTests {

        @SneakyThrows
        @Test
        void shouldReturnFreeLoadingWhenCpuLoadingIsUnavailableInSystem() {
            //when
            doReturn(Double.NaN).when(operatingSystemMXBean).getCpuLoad();

            //do
            List<NodeLoading> nodeLoadings = waitAndGet(
                () -> clusterProvider.currentNodeLoading(),
                list -> !list.isEmpty()
            );


            //verify
            assertThat(nodeLoadings)
                .singleElement()
                .matches(nodeLoading -> nodeLoading.getMedianCpuLoading() == MIN_CPU_LOADING);
        }

        @Test
        void shouldCalculateNodeLoadingProperly() {
            //when
            AtomicInteger callsAtomic = new AtomicInteger(-1);
            double[] mockCpuLoadingValues = new double[]{
                1., 0.9, 0.8, 0.7, 0.6, 0.1, 0.05, //decrease loading
                0.3, 0.6, 0.8 //increase loading again
            };
            doAnswer(invocation -> {
                int curId = callsAtomic.getAndIncrement();
                if (curId < 0 || curId >= mockCpuLoadingValues.length) {
                    return CPU_LOADING_UNDEFINED;
                }
                return mockCpuLoadingValues[curId];
            })
                .when(operatingSystemMXBean).getCpuLoad();

            //do
            waitAndGet(
                callsAtomic::get,
                calls -> calls > mockCpuLoadingValues.length
            );

            //verify
            assertThat(clusterProvider.currentNodeLoading())
                .singleElement()
                .matches(nodeLoading -> nodeLoading.getMedianCpuLoading() == 0.7);
        }

        @Test
        void shouldDetectNodeAsFreeWhenCpuLoadingWasOverloadedAndAfterIsUnavailableInSystem() {
            //when
            AtomicInteger callsAtomic = new AtomicInteger(-1);
            double[] mockCpuLoadingValues = new double[]{
                0.1, 0.99, 0.99, 0.99
            };
            doAnswer(invocation -> {
                int curId = callsAtomic.getAndIncrement();
                if (curId < 0 || curId >= mockCpuLoadingValues.length) {
                    return CPU_LOADING_UNDEFINED;
                }
                return mockCpuLoadingValues[curId];
            })
                .when(operatingSystemMXBean).getCpuLoad();

            //do
            waitAndGet(
                callsAtomic::get,
                calls -> calls > mockCpuLoadingValues.length
            );

            //verify
            waitAndGet(
                () -> clusterProvider.currentNodeLoading(),
                nodeLoading -> nodeLoading.size() == 1 &&
                    nodeLoading.get(0).getMedianCpuLoading() == MIN_CPU_LOADING
            );
        }

        @Test
        void shouldReadNodesLoading() {
            //when
            var firstNode = createNode(0.1);
            var secondNode = createNode(0.5);
            var thirdNode = createNode(0.95);

            //verify
            var nodeLoadings = waitAndGet(
                () -> clusterProvider.currentNodeLoading(),
                n -> n.size() == 4
            );
            assertThat(nodeLoadings)
                .containsAll(List.of(
                    NodeLoading.builder()
                        .node(firstNode.getNode())
                        .medianCpuLoading(0.1)
                        .build(),
                    NodeLoading.builder()
                        .node(secondNode.getNode())
                        .medianCpuLoading(0.5)
                        .build(),
                    NodeLoading.builder()
                        .node(thirdNode.getNode())
                        .medianCpuLoading(0.95)
                        .build()
                ));
        }
    }


    @Nested
    class ClusterCapabilitiesTests {

        @Test
        void shouldExposeEntityWhenCapabilityRegistered() {
            //when
            prepareLocalCapabilities();

            //verify
            List<CapabilityEntity> capabilityEntities = waitAndGet(
                () -> Lists.newArrayList(capabilityRepository.findAll()),
                caps -> caps.size() == 2
            );
            assertThat(capabilityEntities)
                .containsExactlyInAnyOrder(
                    CapabilityEntity.builder()
                        .nodeId(clusterProvider.nodeId())
                        .value(Capabilities.___TEST_1.toString())
                        .build(),
                    CapabilityEntity.builder()
                        .nodeId(clusterProvider.nodeId())
                        .value(Capabilities.___TEST_2.toString())
                        .build()
                );
        }

        @Test
        void shouldNotUpdateCapabilityEntitiesWhenTheyHaveNotBeenChanged() {
            //when
            prepareLocalCapabilities();
            Set<UUID> capabilityEntityIds = waitAndGet(
                () -> Lists.newArrayList(capabilityRepository.findAll()),
                caps -> caps.size() == 2
            ).stream()
                .map(CapabilityEntity::getId)
                .collect(Collectors.toSet());


            //do
            clusterProvider.updateCapabilities();

            //verify
            assertThat(capabilityRepository.findAll())
                .map(CapabilityEntity::getId)
                .containsExactlyInAnyOrderElementsOf(capabilityEntityIds);
        }

        @Test
        void shouldDiscoveryClusterCapabilities() {
            //when
            var firstNode = createNode();
            var secondNode = createNode();

            prepareCapability(firstNode.getNode(), Capabilities.___TEST_1);
            prepareCapability(secondNode.getNode(), Capabilities.___TEST_1);
            prepareCapability(secondNode.getNode(), Capabilities.___TEST_2);

            //verify
            Map<UUID, EnumSet<Capabilities>> clusterCapabilities = waitAndGet(
                () -> clusterProvider.clusterCapabilities(),
                caps -> caps.size() == 2
            );
            assertThat(clusterCapabilities)
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                    firstNode.getNode(), EnumSet.of(Capabilities.___TEST_1),
                    secondNode.getNode(), EnumSet.of(Capabilities.___TEST_1, Capabilities.___TEST_2)
                ));
        }

        @Test
        void shouldCheckDoAllNodesSupportCorrectly() {
            //when
            var firstNode = createNode();
            var secondNode = createNode();

            prepareLocalCapabilities();
            prepareCapability(firstNode.getNode(), Capabilities.___TEST_1);
            prepareCapability(secondNode.getNode(), Capabilities.___TEST_1);
            prepareCapability(secondNode.getNode(), Capabilities.___TEST_2);

            //do
            waitAndGet(
                () -> clusterProvider.clusterCapabilities(),
                caps -> caps.size() == 3
            );

            //verify
            assertThat(clusterProvider.doAllNodesSupport(Capabilities.___TEST_1)).isTrue();
            assertThat(clusterProvider.doAllNodesSupport(Capabilities.___TEST_2)).isFalse();
        }
    }

    private void prepareCapability(UUID nodeId, Capabilities capabilities) {
        var firstNodeFirstCapability = CapabilityEntity.builder()
            .nodeId(nodeId)
            .value(capabilities.toString())
            .build();
        capabilityRepository.save(firstNodeFirstCapability);
    }

    private void prepareLocalCapabilities() {
        var firstSource = mock(CapabilityRegister.class);
        var secondSource = mock(CapabilityRegister.class);

        when(firstSource.capabilities()).thenReturn(EnumSet.of(Capabilities.___TEST_1));
        when(secondSource.capabilities()).thenReturn(EnumSet.of(Capabilities.___TEST_2, Capabilities.UNKNOWN));
        doReturn(List.of(firstSource, secondSource)).when(capabilityRegisterProvider).getAllCapabilityRegister();
    }

    private NodeStateEntity createNode(double cpuLoading) {
        var nodeStateEntity = createNode()
            .toBuilder()
            .medianCpuLoading(cpuLoading)
            .build();
        return nodeStateRepository.save(nodeStateEntity);
    }

    private NodeStateEntity createNode() {
        NodeStateEntity nodeStateEntity = NodeStateEntity.builder()
            .node(UUID.randomUUID())
            .medianCpuLoading(0.1)
            .lastUpdateDateUtc(LocalDateTime.now(clock))
            .build();
        return nodeStateRepository.save(nodeStateEntity);
    }
}