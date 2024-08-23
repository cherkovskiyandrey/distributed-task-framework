package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.models.SagaContext;
import com.distributed_task_framework.saga.persistence.entities.SagaContextEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;
import org.springframework.beans.factory.annotation.Autowired;

@Mapper(
        componentModel = "spring",
        unmappedTargetPolicy = ReportingPolicy.IGNORE
)
public abstract class ContextMapper {
    @Autowired
    protected ObjectMapper objectMapper;

    public abstract SagaContext map(SagaContextEntity sagaContextEntity);

    public abstract SagaContextEntity map(SagaContext sagaContext);

    @SneakyThrows
    public TaskId byteArrayToTaskId(byte[] taskId) {
        return objectMapper.readValue(taskId, TaskId.class);
    }

    @SneakyThrows
    public byte[] taskIdToByteArray(TaskId taskId) {
        return objectMapper.writeValueAsBytes(taskId);
    }
}
