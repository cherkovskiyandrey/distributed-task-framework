package com.distributed_task_framework.service;

import com.fasterxml.jackson.databind.JavaType;

import java.io.IOException;

public interface TaskSerializer {
    /**
     * Read raw message from byte array to DTO.
     *
     * @param message
     * @return
     */
    <T> T readValue(byte[] message, Class<T> cls) throws IOException;

    /**
     * Read raw message from byte array to DTO by any type.
     *
     * @param message
     * @param type
     * @return
     * @param <T>
     * @throws IOException
     */
    <T> T readValue(byte[] message, JavaType type) throws IOException;

    /**
     * Write DTO to byte array.
     *
     * @param dto
     * @return
     */
    <T> byte[] writeValue(T dto) throws IOException;
}
