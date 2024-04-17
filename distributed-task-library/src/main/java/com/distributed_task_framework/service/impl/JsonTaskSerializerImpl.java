package com.distributed_task_framework.service.impl;

import com.distributed_task_framework.service.TaskSerializer;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.io.IOException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
public class JsonTaskSerializerImpl implements TaskSerializer {
    ObjectMapper objectMapper;

    @Override
    public <T> T readValue(byte[] message, Class<T> cls) throws IOException {
        return objectMapper.readValue(message, cls);
    }

    @Override
    public <T> T readValue(byte[] message, JavaType type) throws IOException {
        return objectMapper.readValue(message, type);
    }

    @Override
    public <T> byte[] writeValue(T dto) throws IOException {
        if (dto == null) {
            return null;
        }
        return objectMapper.writeValueAsBytes(dto);
    }
}
