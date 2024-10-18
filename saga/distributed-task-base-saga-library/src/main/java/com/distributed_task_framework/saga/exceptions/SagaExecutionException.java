package com.distributed_task_framework.saga.exceptions;

import jakarta.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class SagaExecutionException extends SagaException {
    @Getter
    private final String rootCauseType;

    public SagaExecutionException(@Nullable String message,
                                  @Nullable Throwable cause,
                                  String rootCauseType) {
        super(message, cause);
        this.rootCauseType = rootCauseType;
    }

    /**
     * If root cause exception class doesn't exist on current node
     * and can't be deserializable here - return false,
     * otherwise - return true.
     *
     * @return
     */
    public boolean hasRecognisableRootCause() {
        return getCause() != null;
    }

    /**
     * Can be useful in case when exception has been moved from one package to another
     * but remain the name.
     *
     * @param exceptionCls
     * @return
     */
    public boolean isTheSameBaseOnSimpleName(Class<? extends Throwable> exceptionCls) {
        return Objects.equals(
                StringUtils.substringAfterLast(rootCauseType, "."),
                exceptionCls.getSimpleName()
        );
    }
}
