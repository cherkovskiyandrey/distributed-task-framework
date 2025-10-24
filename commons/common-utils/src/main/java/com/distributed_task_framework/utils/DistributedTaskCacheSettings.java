package com.distributed_task_framework.utils;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder
public class DistributedTaskCacheSettings {
    @Builder.Default
    Duration expireAfterWrite = null;
    @Builder.Default
    Duration expireAfterAccess = null;
}
