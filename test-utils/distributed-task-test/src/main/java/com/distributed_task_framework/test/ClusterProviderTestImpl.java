package com.distributed_task_framework.test;

import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.NodeLoading;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.service.internal.CapabilityRegisterProvider;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ClusterProviderTestImpl implements ClusterProvider {
    public static final UUID TEST_NODE_ID = UUID.randomUUID();
    public static final Double DEFAULT_CPU_LOADING = 0.01D;

    NodeStateRepository nodeStateRepository;
    CapabilityRegisterProvider capabilityRegisterProvider;
    Clock clock;

    public ClusterProviderTestImpl(NodeStateRepository nodeStateRepository,
                                   CapabilityRegisterProvider capabilityRegisterProvider,
                                   Clock clock) {
        this.nodeStateRepository = nodeStateRepository;
        this.capabilityRegisterProvider = capabilityRegisterProvider;
        this.clock = clock;
        reinit();
    }

    private void reinit() {
        nodeStateRepository.deleteAll();
        nodeStateRepository.save(
            NodeStateEntity.builder()
                .node(TEST_NODE_ID)
                .lastUpdateDateUtc(LocalDateTime.now(clock))
                .medianCpuLoading(DEFAULT_CPU_LOADING)
                .build()
        );
    }

    @Override
    public UUID nodeId() {
        return TEST_NODE_ID;
    }

    @Override
    public boolean isNodeRegistered() {
        return true;
    }

    @Override
    public Set<UUID> clusterNodes() {
        return Set.of(nodeId());
    }

    @Override
    public List<NodeLoading> currentNodeLoading() {
        return List.of(NodeLoading.builder()
            .node(nodeId())
            .medianCpuLoading(DEFAULT_CPU_LOADING)
            .build()
        );
    }

    @Override
    public Map<UUID, EnumSet<Capabilities>> clusterCapabilities() {
        return Map.of(nodeId(), currentNodeCapabilities());
    }

    @Override
    public boolean doAllNodesSupport(Capabilities... capabilities) {
        return currentNodeCapabilities().containsAll(Lists.newArrayList(capabilities));
    }

    private EnumSet<Capabilities> currentNodeCapabilities() {
        return capabilityRegisterProvider.getAllCapabilityRegister().stream()
            .flatMap(capabilityRegister -> capabilityRegister.capabilities().stream())
            .collect(Collectors.collectingAndThen(
                    Collectors.toSet(),
                    EnumSet::copyOf
                )
            );
    }
}
