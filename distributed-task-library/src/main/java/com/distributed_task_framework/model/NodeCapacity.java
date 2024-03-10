package com.distributed_task_framework.model;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;

@Getter
public class NodeCapacity {
    @Nullable //only for FairGeneralTaskPlannerServiceImpl
    private final UUID node;
    private final Set<String> taskNames;
    private int busyCapacity;
    @Setter
    private int freeCapacity;

    public NodeCapacity(UUID node, Set<String> taskNames, int maxParallelTasksInNode) {
        this.node = node;
        this.taskNames = taskNames;
        this.freeCapacity = maxParallelTasksInNode;
        this.busyCapacity = 0;
    }

    public void busyOnlyOne() {
        busy(1);
    }

    public void busy(int busyCapacity) {
        freeCapacity = Math.max(freeCapacity - busyCapacity, 0);
        this.busyCapacity += busyCapacity;
    }
}
