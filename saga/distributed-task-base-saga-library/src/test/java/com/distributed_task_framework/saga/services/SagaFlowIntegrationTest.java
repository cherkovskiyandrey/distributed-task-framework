package com.distributed_task_framework.saga.services;

import com.distributed_task_framework.saga.BaseSpringIntegrationTest;
import com.distributed_task_framework.saga.exceptions.SagaNotFoundException;
import com.distributed_task_framework.saga.generator.TestSagaModelSpec;
import com.distributed_task_framework.saga.settings.SagaSettings;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

//todo
public class SagaFlowIntegrationTest extends BaseSpringIntegrationTest {

    //todo: SagaNotFoundException when is too late
    //todo: TimeoutException when timeout expired
    //todo: sync/async/status/wait
    //todo: cancelation
    //todo: saga expiration

    @Nested
    public class WaitCompletion {



        //todo: shouldThrowSagaNotFoundExceptionWhenCompletedAndRemoved
    }

    @Nested
    public class GetCompletion {
        //todo
    }

    @Nested
    public class IsCompleted {
        //todo
    }

    @Nested
    public class Cancel {
        //todo

        //todo: SagaCancellationException in get() method
    }


}
