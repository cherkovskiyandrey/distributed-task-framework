package com.distributed_task_framework.saga.test_service.services;

import com.distributed_task_framework.saga.test_service.models.RemoteOneDto;

public interface RemoteServiceOne {
    RemoteOneDto create(RemoteOneDto remoteOneDto);
    void delete(String remoteOneId);
}
