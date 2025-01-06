package com.distributed_task_framework.saga.utils;

import com.distributed_task_framework.saga.exceptions.SagaMethodParseException;
import com.distributed_task_framework.saga.models.SagaMethod;
import com.google.common.collect.Lists;
import lombok.experimental.UtilityClass;

import java.lang.invoke.SerializedLambda;
import java.util.List;

@UtilityClass
public class SerializableLambdaSagaMethodFactory {

    public static SagaMethod of(SerializedLambda serializedLambda) {
        var signature = parseSignature(serializedLambda.getImplMethodSignature());
        return new SagaMethod(
            asClass(serializedLambda.getImplClass()),
            serializedLambda.getImplMethodName(),
            signature.parameters(),
            signature.returnType()
        );
    }

    @SuppressWarnings("ConstantValue")
    private static Signature parseSignature(String signature) {
        String returnType = null;
        List<String> parameters = Lists.newArrayList();

        boolean isMethod = false;
        int arrays = 0;
        Boolean isParameters = null;
        var buffer = new StringBuilder();

        for (int i = 0; i < signature.length(); ++i) {
            var ch = signature.charAt(i);
            if (isMethod && ch != ';') {
                buffer.append(ch == '/' ? '.' : ch);
            } else if (isMethod && ch == ';') {
                addArraySuffix(buffer, arrays);
                if (isParameters != null && isParameters) {
                    parameters.add(buffer.toString());
                } else {
                    returnType = buffer.toString();
                }
                arrays = 0;
                isMethod = false;
                buffer = new StringBuilder();
            } else if (ch == '(') {
                if (isParameters == null) {
                    isParameters = true;
                } else {
                    throw new SagaMethodParseException("Can't parse methodSignature=[%s]".formatted(signature));
                }
            } else if (ch == ')') {
                if (isParameters != null && isParameters) {
                    isParameters = false;
                } else {
                    throw new SagaMethodParseException("Can't parse methodSignature=[%s]".formatted(signature));
                }
            } else if (ch == 'L') {
                isMethod = true;
            } else if (ch == '[') {
                arrays++;
            } else {
                var outputType = switch (ch) {
                    case 'V' -> void.class.getName();
                    case 'I' -> int.class.getName();
                    case 'Z' -> boolean.class.getName();
                    case 'B' -> byte.class.getName();
                    case 'C' -> char.class.getName();
                    case 'D' -> double.class.getName();
                    case 'F' -> float.class.getName();
                    case 'J' -> long.class.getName();
                    case 'S' -> short.class.getName();
                    default -> null;
                };
                buffer.append(outputType);
                addArraySuffix(buffer, arrays);
                if (isParameters != null && isParameters) {
                    parameters.add(buffer.toString());
                } else {
                    returnType = buffer.toString();
                }
                arrays = 0;
                buffer = new StringBuilder();
            }
        }
        return new Signature(returnType, parameters);
    }

    private static void addArraySuffix(StringBuilder buffer, int arrays) {
        buffer.append("[]".repeat(Math.max(0, arrays)));
    }

    private static String asClass(String type) {
        return type.replace('/', '.');
    }

    private record Signature(
        String returnType,
        List<String> parameters
    ) {
    }
}
