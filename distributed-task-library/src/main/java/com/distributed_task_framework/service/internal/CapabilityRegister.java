package com.distributed_task_framework.service.internal;

import com.distributed_task_framework.model.Capabilities;

import java.util.EnumSet;

/**
 * Any service/bean/component can implement this interface
 * in order to declare own capabilities. Which have be discovered by any node in cluster.
 */
public interface CapabilityRegister {
    /**
     * Return own capabilities
     *
     * @return
     */
    EnumSet<Capabilities> capabilities();
}
