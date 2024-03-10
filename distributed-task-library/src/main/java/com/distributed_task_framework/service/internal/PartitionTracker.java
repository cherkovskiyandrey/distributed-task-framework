package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.Partition;

import java.util.Set;

public interface PartitionTracker {

    void reinit();

    void track(Partition partition);

    void track(Set<Partition> partitions);

    Set<Partition> getAll();

    void gcIfNecessary();

    void compactIfNecessary();
}
