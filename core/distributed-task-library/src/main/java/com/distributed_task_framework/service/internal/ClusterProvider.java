package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.Capabilities;
import com.distributed_task_framework.model.NodeLoading;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface ClusterProvider {

    UUID nodeId();

    Set<UUID> clusterNodes();

    List<NodeLoading> currentNodeLoading();

    Map<UUID, EnumSet<Capabilities>> clusterCapabilities();

    boolean doAllNodesSupport(Capabilities... capabilities);

    boolean doAllNodesSupportOrEmpty(Capabilities... capabilities);
}
