package com.distributed_task_framework.saga.mappers;

import com.distributed_task_framework.saga.exceptions.SagaInternalException;
import com.distributed_task_framework.saga.exceptions.SagaParseObjectException;
import com.distributed_task_framework.saga.models.SagaParsedTrackId;
import com.distributed_task_framework.saga.models.SagaTrackId;
import com.distributed_task_framework.service.TaskSerializer;

import java.io.IOException;
import java.util.Base64;


public class SagaTrackIdMapper {
    private final TaskSerializer taskSerializer;

    public SagaTrackIdMapper(TaskSerializer taskSerializer) {
        this.taskSerializer = taskSerializer;
    }

    public SagaParsedTrackId mapToParsed(SagaTrackId trackId) {
        byte[] decoded = Base64.getUrlDecoder().decode(trackId.trackId());
        try {
            return taskSerializer.readValue(decoded, SagaParsedTrackId.class);
        } catch (IOException e) {
            throw new SagaParseObjectException("Couldn't parse trackId=[%s]".formatted(trackId), e);
        }
    }

    public SagaTrackId map(SagaParsedTrackId sagaParsedTrackId) {
        try {
            byte[] encoded = taskSerializer.writeValue(sagaParsedTrackId);
            return new SagaTrackId(Base64.getUrlEncoder().encodeToString(encoded));
        } catch (IOException e) {
            throw new SagaInternalException(e);
        }
    }
}
