package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.Partition;
import io.micrometer.core.annotation.Timed;

import java.util.Set;

@Timed("partition_tracker")
public interface PartitionTracker {

    @Timed(extraTags = "reinit")
    void reinit();

    @Timed(extraTags = "track")
    void track(Partition partition);

    @Timed(extraTags = "track_set")
    void track(Set<Partition> partitions);

    @Timed(extraTags = "getAll")
    Set<Partition> getAll();

    @Timed(extraTags = "gcIfNecessary")
    void gcIfNecessary();

    @Timed(extraTags = "compactIfNecessary")
    void compactIfNecessary();
}
