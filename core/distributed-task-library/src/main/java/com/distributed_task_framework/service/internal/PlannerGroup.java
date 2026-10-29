package com.distributed_task_framework.service.internal;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum PlannerGroup {
    DEFAULT("default"),
    JOIN("join"),
    VQB_MANAGER("vqb_manager"),
    @VisibleForTesting
    TEST_GROUP_NAME( "dummy_group");
    private final String name;
}
