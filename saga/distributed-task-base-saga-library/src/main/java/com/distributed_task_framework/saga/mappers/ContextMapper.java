package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.model.TaskId;
import com.distributed_task_framework.saga.models.Saga;
import com.distributed_task_framework.saga.models.SagaPipeline;
import com.distributed_task_framework.saga.persistence.entities.DlsSagaEntity;
import com.distributed_task_framework.saga.persistence.entities.SagaEntity;
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

    public abstract Saga toModel(SagaEntity sagaEntity);

    public abstract SagaEntity toEntity(Saga sagaContext);

    public abstract DlsSagaEntity mapToDls(SagaEntity sagaEntity);

    @SneakyThrows
    public TaskId byteArrayToTaskId(byte[] taskId) {
        return objectMapper.readValue(taskId, TaskId.class);
    }

    @SneakyThrows
    public byte[] taskIdToByteArray(TaskId taskId) {
        return objectMapper.writeValueAsBytes(taskId);
    }

    @SneakyThrows
    public SagaPipeline byteArrayToSagaEmbeddedPipelineContext(byte[] context) {
        return objectMapper.readValue(context, SagaPipeline.class);
    }

    @SneakyThrows
    public byte[] sagaEmbeddedPipelineContextToByteArray(SagaPipeline context) {
        return objectMapper.writeValueAsBytes(context);
    }
}
