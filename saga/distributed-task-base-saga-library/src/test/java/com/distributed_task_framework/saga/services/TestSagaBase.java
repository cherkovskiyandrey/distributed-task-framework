package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.exceptions.SagaExecutionException;
import com.distributed_task_framework.saga.exceptions.TestUserUncheckedException;
import com.distributed_task_framework.saga.generator.Revert;
import jakarta.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Getter
@AllArgsConstructor
class TestSagaBase {
    protected int value;

    // ----- Sum ---------------------
    public void sumAsConsumer(int input) {
        value += input;
    }

    @Revert
    public void diffForConsumer(int input, @Nullable SagaExecutionException throwable) {
        value -= input;
    }
    //--------------------------------


    // ----- Sum with exception ------
    public void sumAsConsumerWithException(int input) {
        sumAsConsumer(input);
        throw new TestUserUncheckedException();
    }

    @Revert
    public void diffForConsumerWithExceptionHandling(int input, @Nullable SagaExecutionException throwable) {
        assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
        diffForConsumer(input, throwable);
    }
    //--------------------------------

    // ----- Sum as function ------
    public int sumAsFunction(int input) {
        value += input;
        return value;
    }

    @Revert
    public void diffForFunction(int input, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
        value -= input;
    }
    //--------------------------------

    // ----- Sum as function with exception ------
    public int sumAsFunctionWithException(int input) {
        sumAsFunction(input);
        throw new TestUserUncheckedException();
    }

    @Revert
    public void diffForFunctionWithExceptionHandling(int input, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
        assertThat(output).isNull();
        assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
        diffForFunction(input, output, throwable);
    }
    //--------------------------------

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

    // ----- Multiply ---------------------
    public int multiplyAsFunction(int parentOutput) {
        value *= parentOutput;
        return value;
    }

    @Revert
    public void divideForFunction(int parentOutput, @Nullable Integer output, @Nullable SagaExecutionException throwable) {
        value /= parentOutput;
    }
    // ------------------------------------

    // ----- Multiply with exception ------
    public int multiplyAsFunctionWithException(int parentOutput) {
        value *= parentOutput;
        throw new TestUserUncheckedException();
    }

    @Revert
    public void divideForFunctionWithExceptionHandling(int parentOutput,
                                                       @Nullable Integer output,
                                                       @Nullable SagaExecutionException throwable) {
        assertThat(output).isNull();
        assertThat(throwable).hasCauseInstanceOf(TestUserUncheckedException.class);
        divideForFunction(parentOutput, output, throwable);
    }
    // ---------------------------------------

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

    public void justThrowExceptionAsConsumer(int input) {
        throw new TestUserUncheckedException();
    }

    public int justThrowExceptionAsFunction(int input) {
        throw new TestUserUncheckedException();
    }

    @SneakyThrows
    public int sleep(int i) {
        TimeUnit.MILLISECONDS.sleep(i);
        return i;
    }
}
