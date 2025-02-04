package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.Revert;
import jakarta.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
class TestSagaBase {
    protected int value;

    public void sumAsConsumer(int input) {
        value += input;
    }

    @Revert
    public void diffForConsumer(int input, @Nullable SagaExecutionException throwable) {
        value -= input;
    }

    public int sumAsFunction(int input) {
        value += input;
        return value;
    }

    @Revert
    public void diffForFunction(int input, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
        value -= input;
    }

    public void multiplyAsConsumer(int parentOutput) {
        value *= parentOutput;
    }

    @Revert
    public void divideForConsumer(int parentOutput, @Nullable SagaExecutionException throwable) {
        value /= parentOutput;
    }

    public void multiplyAsConsumerWithRootInput(int parentOutput, int input) {
        value *= input * parentOutput;
    }

    @Revert
    public void divideForConsumerWithRootInput(int parentOutput,
                                               int rootInput,
                                               SagaExecutionException throwable) {
        value /= parentOutput * rootInput;
    }

    public int multiplyAsFunction(int parentOutput) {
        value *= parentOutput;
        return value;
    }

    @Revert
    public void divideForFunction(int parentOutput, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
        value /= parentOutput;
    }

    public int multiplyAsFunctionWithRootInput(int parentOutput, int rootInput) {
        value *= parentOutput * rootInput;
        return value;
    }

    @Revert
    public void divideForFunctionWithRootInput(int parentOutput,
                                               int rootInput,
                                               @Nullable Integer output,
                                               @Nullable SagaExecutionException throwable) {
        value /= parentOutput * rootInput;
    }

    public void justThrowException(int input) {
        throw new TestUserUncheckedException();
    }
}
