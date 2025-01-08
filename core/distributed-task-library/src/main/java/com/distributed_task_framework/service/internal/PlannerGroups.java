package com.distributed_task_framework.service.internal;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum PlannerGroups {
    DEFAULT("default"),
    JOIN("join"),
    VQB_MANAGER("vqb_manager");
    private final String name;
}
