package com.distributed_task_framework.service.internal;

public interface PlannerStateRegistry {

    void markActive(PlannerGroup plannerGroup);

    void markInactive(PlannerGroup plannerGroup);
}
