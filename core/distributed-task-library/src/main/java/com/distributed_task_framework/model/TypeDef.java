package com.distributed_task_framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.Value;

import java.lang.reflect.Type;

@Value
public class TypeDef<T> {
    private static final TypeFactory TYPE_FACTORY = new ObjectMapper().getTypeFactory();

    JavaType javaType;

    public static <T> TypeDef<T> of(Class<T> cls) {
        return new TypeDef<>(asJavaType(cls));
    }

    public static <T> TypeDef<T> of(TypeReference<T> typeReference) {
        return new TypeDef<>(asJavaType(typeReference.getType()));
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public static <T> TypeDef<T> of(@JsonProperty("javaType") JavaType javaType) {
        return new TypeDef<>(javaType);
    }

    private static JavaType asJavaType(Type type) {
        return TYPE_FACTORY.constructType(type);
    }
}
