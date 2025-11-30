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

    boolean isNodeRegistered();

    Set<UUID> clusterNodes();

    List<NodeLoading> currentNodeLoading();

    Map<UUID, EnumSet<Capabilities>> clusterCapabilities();

    /**
     * IMPORTANT: take into account that this method has a time window to detect a new node or lost node from the cluster
     * (default value is 1 sec.)
     *
     * @param capabilities
     * @return
     */
    boolean doAllNodesSupport(Capabilities... capabilities);
}
