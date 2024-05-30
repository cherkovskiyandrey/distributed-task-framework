package com.distributed_task_framework.model;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.EnumSet;

public enum Capabilities {
    VIRTUAL_QUEUE_MANAGER_PLANNER_V1,
    VIRTUAL_QUEUE_BASE_FAIR_TASK_PLANNER_V1,
    UNKNOWN,

    @VisibleForTesting
    ___TEST_1,
    @VisibleForTesting
    ___TEST_2;

    public static Capabilities from(String capabilityStr) {
        return Arrays.stream(Capabilities.values())
                .filter(cap -> cap.toString().equals(capabilityStr))
                .findFirst()
                .orElse(Capabilities.UNKNOWN);
    }

    public static EnumSet<Capabilities> createEmpty() {
        return EnumSet.noneOf(Capabilities.class);
    }
}
