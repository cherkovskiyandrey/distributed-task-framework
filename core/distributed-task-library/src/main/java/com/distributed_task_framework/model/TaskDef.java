package com.distributed_task_framework.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.With;

import java.lang.reflect.Type;

//Generic is used for type safety
@SuppressWarnings("unused")
@Value
public class TaskDef<T> {
    private static final TypeFactory TYPE_FACTORY = new ObjectMapper().getTypeFactory();

    @With
    String appName;
    String taskName;
    @EqualsAndHashCode.Exclude
    JavaType inputMessageType;

    public static <T> TaskDef<T> privateTaskDef(String taskName, Class<T> inputMessageClass) {
        return new TaskDef<>(null, taskName, asJavaType(inputMessageClass));
    }

    public static TaskDef<Void> privateTaskDef(String taskName) {
        return new TaskDef<>(null, taskName, asJavaType(Void.class));
    }

    /**
     * Has to be used for generic types.
     *
     * @param taskName
     * @param inputMessageClass
     * @return
     * @param <T>
     */
    public static <T> TaskDef<T> privateTaskDef(String taskName, TypeReference<T> inputMessageClass) {
        return new TaskDef<>(null, taskName, asJavaType(inputMessageClass.getType()));
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public static <T> TaskDef<T> publicTaskDef(@JsonProperty("appName") String appName,
                                               @JsonProperty("taskName") String taskName,
                                               @JsonProperty("inputMessageType") JavaType inputMessageClass) {
        return new TaskDef<>(appName, taskName, inputMessageClass);
    }

    public static <T> TaskDef<T> publicTaskDef(String appName, String taskName, Class<? extends T> inputMessageClass) {
        return new TaskDef<>(appName, taskName, asJavaType(inputMessageClass));
    }

    private static JavaType asJavaType(Type type) {
        return TYPE_FACTORY.constructType(type);
    }
}
