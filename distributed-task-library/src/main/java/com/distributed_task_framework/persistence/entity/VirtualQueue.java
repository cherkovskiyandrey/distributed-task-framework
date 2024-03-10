package com.distributed_task_framework.persistence.entity;

public enum VirtualQueue {
    /**
     * All newcomer tasks all into this queue.
     * Exception: task with the same affinityGroup + affinity is created from parent task.
     */
    NEW,

    /**
     * Tasks are ready to be planned. Don't have tasks with the same affinityGroup + affinity and different workflowId.
     */
    READY,

    /**
     * Tasks which have to wait for their queue. Because there are tasks with same affinityGroup + affinity,
     * different workflowId and earliest workflowCreatedDate in the READY queue.
     */
    PARKED,

    /**
     * Tasks which have been completed and have to be deleted.
     */
    DELETED
}
