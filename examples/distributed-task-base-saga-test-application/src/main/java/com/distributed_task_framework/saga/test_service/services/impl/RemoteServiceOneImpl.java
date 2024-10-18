package com.distributed_task_framework.saga.test_service.services.impl;

import com.distributed_task_framework.saga.test_service.models.RemoteOneDto;
import com.distributed_task_framework.saga.test_service.services.RemoteServiceOne;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;


@Slf4j
@Component
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class RemoteServiceOneImpl implements RemoteServiceOne {
    //todo: save in db via rest on local node
    Map<String, RemoteOneDto> repository = Maps.newHashMap();

    @Override
    public RemoteOneDto create(RemoteOneDto remoteOneDto) {
        repository.put(remoteOneDto.getRemoteOneId(), remoteOneDto);
        return remoteOneDto;
    }

    @Override
    public void delete(String remoteOneId) {
        repository.remove(remoteOneId);
    }
}
