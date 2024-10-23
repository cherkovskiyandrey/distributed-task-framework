package com.distributed_task_framework.saga.test_service.services;

import com.distributed_task_framework.saga.test_service.models.RemoteTwoDto;

public interface RemoteServiceTwo {
    RemoteTwoDto create(RemoteTwoDto remoteTwoDto);
    void delete(String remoteTwoId);
}
