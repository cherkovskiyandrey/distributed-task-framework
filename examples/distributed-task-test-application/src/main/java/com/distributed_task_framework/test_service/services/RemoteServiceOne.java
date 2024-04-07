package com.distributed_task_framework.test_service.services;

import com.distributed_task_framework.test_service.models.RemoteOneDto;

public interface RemoteServiceOne {
    RemoteOneDto create(RemoteOneDto remoteOneDto);
    void delete(String remoteOneId);
}
