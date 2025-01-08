package com.distributed_task_framework.saga;

import com.distributed_task_framework.saga.annotations.SagaMethod;
import com.distributed_task_framework.saga.annotations.SagaRevertMethod;
import com.distributed_task_framework.saga.exceptions.SagaMethodNotFoundException;
import com.distributed_task_framework.utils.ReflectionHelper;
import jakarta.annotation.Nullable;
import lombok.experimental.UtilityClass;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UtilityClass
public class SagaNamingUtils {
    private static final String TASK_PREFIX = "_____SAGA";
    private static final String TASK_REVERT_PREFIX = "_____SAGA_REVERT";
    private static final String TASK_NAME_DELIMITER = "_";
    private static final String VERSION_PREFIX = "v";

    public String taskNameFor(Method method) {
        return generateName(method, TASK_PREFIX, TASK_REVERT_PREFIX);
    }

    public String taskNameFor(SagaMethod sagaMethodAnnotation) {
        return generateName(sagaMethodAnnotation, TASK_PREFIX);
    }

    public String taskNameFor(SagaRevertMethod sagaRevertMethod) {
        return generateRevertName(sagaRevertMethod, TASK_REVERT_PREFIX);
    }

    public String sagaMethodNameFor(Method method) {
        return generateName(method, null, null);
    }

    private String generateName(Method method, @Nullable String taskPrefix, @Nullable String revertTaskPrefix) {
        return ReflectionHelper.findAnnotation(method, SagaMethod.class)
            .map(sagaMethod -> generateName(sagaMethod, taskPrefix))
            .or(() -> ReflectionHelper.findAnnotation(method, SagaRevertMethod.class)
                .map(sagaRevertMethod -> generateRevertName(sagaRevertMethod, revertTaskPrefix))
            )
            .orElseThrow(() -> new SagaMethodNotFoundException(
                    "Method=[%s] isn't marked neither [%s], neither [%s]".formatted(
                        method.toString(),
                        SagaMethod.class.getSimpleName(),
                        SagaRevertMethod.class.getSimpleName()
                    )
                )
            );
    }

    private String generateName(SagaMethod sagaMethodAnnotation, @Nullable String taskPrefix) {
        String name = sagaMethodAnnotation.name();
        int version = sagaMethodAnnotation.version();

        return Stream.of(taskPrefix, name, v(version))
            .filter(Objects::nonNull)
            .collect(Collectors.joining(TASK_NAME_DELIMITER));
    }

    private String generateRevertName(SagaRevertMethod sagaRevertMethodAnnotation, @Nullable String revertTaskPrefix) {
        String name = sagaRevertMethodAnnotation.name();
        int version = sagaRevertMethodAnnotation.version();

        return Stream.of(revertTaskPrefix, name, v(version))
            .filter(Objects::nonNull)
            .collect(Collectors.joining(TASK_NAME_DELIMITER));
    }

    @Nullable
    private static String v(int version) {
        return version != 0 ? VERSION_PREFIX + version : null;
    }
}