package com.distributed_task_framework.test;

import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.NodeLoading;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;
import com.distributed_task_framework.persistence.repository.NodeStateRepository;
import com.distributed_task_framework.service.internal.ClusterProvider;
import com.distributed_task_framework.utils.DistributedTaskServiceLifecycle;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multisets;
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


@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ClusterProviderTestImpl implements ClusterProvider, DistributedTaskServiceLifecycle {
    public static final UUID TEST_NODE_ID = UUID.randomUUID();
    public static final Double DEFAULT_CPU_LOADING = 0.01D;

    NodeStateRepository nodeStateRepository;
    ConcurrentHashMultiset<Capabilities> nodeCapabilities = ConcurrentHashMultiset.create();
    Clock clock;

    public ClusterProviderTestImpl(NodeStateRepository nodeStateRepository, Clock clock) {
        this.nodeStateRepository = nodeStateRepository;
        this.clock = clock;
    }

    @Override
    public void start() throws Exception {
        log.info("start()");
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
        return Map.of(nodeId(), EnumSet.copyOf(nodeCapabilities));
    }

    @Override
    public boolean doAllNodesSupport(Capabilities... capabilities) {
        return nodeCapabilities.containsAll(Lists.newArrayList(capabilities));
    }

    @Override
    public void registerCapabilities(EnumSet<Capabilities> capabilities) {
        nodeCapabilities.addAll(capabilities);
    }

    @Override
    public void unregisterCapabilities(EnumSet<Capabilities> capabilities) {
        Multisets.removeOccurrences(nodeCapabilities, capabilities);
    }
}
