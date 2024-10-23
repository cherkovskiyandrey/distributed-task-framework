package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.CapabilityEntity;
import com.distributed_task_framework.persistence.entity.NodeStateEntity;

import java.util.List;
import java.util.Map;

public interface NodeStateExtendedRepository {

    Map<NodeStateEntity, List<CapabilityEntity>> getAllWithCapabilities();
}
