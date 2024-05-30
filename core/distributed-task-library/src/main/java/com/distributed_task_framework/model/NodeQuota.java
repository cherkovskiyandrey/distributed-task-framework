package com.distributed_task_framework.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.util.UUID;

@Getter
@FieldDefaults(level = AccessLevel.PRIVATE)
public class NodeQuota {
    final UUID node;
    int quota;

    public NodeQuota(UUID node, int quota) {
        this.node = node;
        this.quota = quota;
    }

    public NodeQuota incrementQuota() {
        quota = quota + 1;
        return this;
    }
}
