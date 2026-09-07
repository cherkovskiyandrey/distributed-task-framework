package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.service.PlannerState;
import com.distributed_task_framework.service.internal.PlannerGroup;
import com.distributed_task_framework.service.internal.PlannerStateRegistry;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@NoArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class PlannerStateImpl implements PlannerStateRegistry, PlannerState {
    Set<PlannerGroup> registry = ConcurrentHashMap.newKeySet();

    @Override
    public void markActive(PlannerGroup plannerGroup) {
        registry.add(plannerGroup);
    }

    @Override
    public void markInactive(PlannerGroup plannerGroup) {
        registry.remove(plannerGroup);
    }

    @Override
    public boolean isActive(PlannerGroup plannerGroup) {
        return registry.contains(plannerGroup);
    }
}
