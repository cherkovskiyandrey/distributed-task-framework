package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.functions.SagaBiConsumer;
import com.distributed_task_framework.saga.functions.SagaBiFunction;
import com.distributed_task_framework.saga.models.SagaMethod;
import org.junit.jupiter.params.provider.Arguments;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class SagaMethodBase {
    protected static final SagaMethodProvider sagaMethodProvider = new SagaMethodProvider();

    protected static Stream<Arguments> consumerProvider() {
        return Stream.of(
            Arguments.of(
                (SagaBiConsumer<Double, Integer>) sagaMethodProvider::method_double_int,
                lookupMethod("method_double_int"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_double_int",
                    List.of(double.class.getSimpleName(), int.class.getSimpleName()),
                    void.class.getSimpleName()
                )
            ),
            Arguments.of(
                (SagaBiConsumer<double[], int[][][]>) sagaMethodProvider::method_double_array_int_array3,
                lookupMethod("method_double_array_int_array3"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_double_array_int_array3",
                    List.of(double[].class.toGenericString(), int[][][].class.toGenericString()),
                    void.class.getSimpleName()
                )
            ),
            Arguments.of(
                (SagaBiConsumer<double[], String>) sagaMethodProvider::method_double_array_string,
                lookupMethod("method_double_array_string"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_double_array_string",
                    List.of(double[].class.toGenericString(), String.class.getTypeName()),
                    void.class.getSimpleName()
                )
            )
        );
    }

    protected static Stream<Arguments> functionConsumer() {
        return Stream.of(
            Arguments.of(
                (SagaBiFunction<float[][], String[][], Void>) sagaMethodProvider::method_float_array2_string_array2_void,
                lookupMethod("method_float_array2_string_array2_void"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_float_array2_string_array2_void",
                    List.of(float[][].class.toGenericString(), String[][].class.toGenericString()),
                    Void.class.getTypeName()
                )
            ),
            Arguments.of(
                (SagaBiFunction<String, List<Integer>, Integer>) sagaMethodProvider::method_string_list_int,
                lookupMethod("method_string_list_int"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_string_list_int",
                    List.of(String.class.getTypeName(), List.class.getTypeName()),
                    int.class.getTypeName()
                )
            ),
            Arguments.of(
                (SagaBiFunction<Map<String, List<Integer>>, Integer, Integer>) sagaMethodProvider::method_map_integer_integer,
                lookupMethod("method_map_integer_integer"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_map_integer_integer",
                    List.of(Map.class.getTypeName(), Integer.class.getTypeName()),
                    Integer.class.getTypeName()
                )
            ),
            Arguments.of(
                (SagaBiFunction<String, Integer, String>) sagaMethodProvider::method_string_integer_string,
                lookupMethod("method_string_integer_string"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_string_integer_string",
                    List.of(String.class.getTypeName(), Integer.class.getTypeName()),
                    String.class.getTypeName()
                )
            ),
            Arguments.of(
                (SagaBiFunction<String, Integer, String[]>) sagaMethodProvider::method_string_integer_string_array,
                lookupMethod("method_string_integer_string_array"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_string_integer_string_array",
                    List.of(String.class.getTypeName(), Integer.class.getTypeName()),
                    String[].class.toGenericString()
                )
            ),
            Arguments.of(
                (SagaBiFunction<String, Integer, String[][]>) sagaMethodProvider::method_string_integer_string_array2,
                lookupMethod("method_string_integer_string_array2"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_string_integer_string_array2",
                    List.of(String.class.getTypeName(), Integer.class.getTypeName()),
                    String[][].class.toGenericString()
                )
            ),
            Arguments.of(
                (SagaBiFunction<String, Integer, List<String>>) sagaMethodProvider::method_string_integer_list,
                lookupMethod("method_string_integer_list"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_string_integer_list",
                    List.of(String.class.getTypeName(), Integer.class.getTypeName()),
                    List.class.getTypeName()
                )
            ),
            Arguments.of(
                (SagaBiFunction<String[][], Integer[], Map<String, String>>) sagaMethodProvider::method_string_array2_integer_array_map,
                lookupMethod("method_string_array2_integer_array_map"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_string_array2_integer_array_map",
                    List.of(String[][].class.toGenericString(), Integer[].class.toGenericString()),
                    Map.class.getTypeName()
                )
            ),
            Arguments.of(
                (SagaBiFunction<String,
                    SagaMethodProvider.InternalTest<List<String>, Map<String, String>>,
                    SagaMethodProvider.InternalTest<List<String>, Map<String, String>>>) sagaMethodProvider::method_string_custom_custom,
                lookupMethod("method_string_custom_custom"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_string_custom_custom",
                    List.of(String.class.getTypeName(), SagaMethodProvider.InternalTest.class.getTypeName()),
                    SagaMethodProvider.InternalTest.class.getTypeName()
                )
            )
        );
    }

    protected static Stream<Arguments> functionSupplier() {
        return Stream.of(
            Arguments.of(
                (SerializableLambdaSagaMethodFactoryTest.DebuggableSupplier<Map<String, String>[]>) sagaMethodProvider::method_map_array,
                lookupMethod("method_map_array"),
                new SagaMethod(
                    SagaMethodProvider.class.getTypeName(),
                    "method_map_array",
                    List.of(),
                    Map[].class.getTypeName()
                )
            )
        );
    }

    private static Method lookupMethod(String method) {
        return Arrays.stream(sagaMethodProvider.getClass().getDeclaredMethods())
            .filter(m -> Objects.equals(m.getName(), method))
            .findFirst()
            .orElseThrow();
    }

    @FunctionalInterface
    public interface DebuggableSupplier<T> extends Supplier<T>, Serializable {
    }
}
