package com.distributed_task_framework.service;

import com.distributed_task_framework.service.internal.PlannerGroup;

public interface PlannerState {

    /**
     * Determine is planner by {@link PlannerGroup} active on current node.
     *
     * @param plannerGroup
     * @return
     */
    boolean isActive(PlannerGroup plannerGroup);
}
