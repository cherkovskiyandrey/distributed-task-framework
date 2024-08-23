package com.distributed_task_framework.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import com.distributed_task_framework.controller.dto.CommandDto;
import com.distributed_task_framework.controller.dto.CommandListDto;
import com.distributed_task_framework.persistence.entity.DlcEntity;
import com.distributed_task_framework.persistence.entity.RemoteCommandEntity;

import java.util.Collection;
import java.util.stream.Collectors;

@Mapper(
        unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public interface CommandMapper {

    CommandDto map(RemoteCommandEntity remoteCommandEntity);

    default CommandListDto mapToList(Collection<RemoteCommandEntity> remoteCommandEntities) {
        return CommandListDto.builder()
                .commands(remoteCommandEntities.stream().map(this::map).collect(Collectors.toList()))
                .build();
    }

    DlcEntity mapToDlc(RemoteCommandEntity commandEntities);

    default Collection<DlcEntity> mapToDlcList(Collection<RemoteCommandEntity> commandEntities) {
        return commandEntities.stream()
                .map(this::mapToDlc)
                .collect(Collectors.toList());
    }
}
