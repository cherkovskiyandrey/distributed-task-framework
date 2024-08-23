package com.distributed_task_framework.model;

import java.util.UUID;

public record PlannedTask(String affinityGroup, String taskName, UUID workerId) {
}
