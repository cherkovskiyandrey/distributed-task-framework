package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.models.SagaContext;
import com.distributed_task_framework.saga.models.SagaEmbeddedPipelineContext;
import com.distributed_task_framework.saga.persistence.entities.DlsSagaContextEntity;
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

    public abstract SagaContext toModel(SagaContextEntity sagaContextEntity);

    public abstract SagaContextEntity toEntity(SagaContext sagaContext);

    public abstract DlsSagaContextEntity mapToDls(SagaContextEntity sagaContextEntity);

    @SneakyThrows
    public TaskId byteArrayToTaskId(byte[] taskId) {
        return objectMapper.readValue(taskId, TaskId.class);
    }

    @SneakyThrows
    public byte[] taskIdToByteArray(TaskId taskId) {
        return objectMapper.writeValueAsBytes(taskId);
    }

    @SneakyThrows
    public SagaEmbeddedPipelineContext byteArrayToSagaEmbeddedPipelineContext(byte[] context) {
        return objectMapper.readValue(context, SagaEmbeddedPipelineContext.class);
    }

    @SneakyThrows
    public byte[] sagaEmbeddedPipelineContextToByteArray(SagaEmbeddedPipelineContext context) {
        return objectMapper.writeValueAsBytes(context);
    }
}
