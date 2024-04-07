package com.distributed_task_framework.test_service.services.impl;

import com.distributed_task_framework.test_service.models.RemoteTwoDto;
import com.distributed_task_framework.test_service.services.RemoteServiceTwo;
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
public class RemoteServiceTwoImpl implements RemoteServiceTwo {
    //todo: save in db via rest on local node
    Map<String, RemoteTwoDto> repository = Maps.newHashMap();

    @Override
    public RemoteTwoDto create(RemoteTwoDto remoteTwoDto) {
        repository.put(remoteTwoDto.getRemoteTwoId(), remoteTwoDto);
        return remoteTwoDto;
    }

    @Override
    public void delete(String remoteTwoId) {
        repository.remove(remoteTwoId);
    }
}
