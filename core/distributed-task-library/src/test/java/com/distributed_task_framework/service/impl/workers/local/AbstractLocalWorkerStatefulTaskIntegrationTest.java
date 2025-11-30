package com.distributed_task_framework.service.impl.workers.local;

import com.distributed_task_framework.task.TestStatefulTaskModelSpec;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
public abstract class AbstractLocalWorkerStatefulTaskIntegrationTest extends BaseLocalWorkerIntegrationTest {

    public record State(
        String field1,
        Integer field2
    ) {
    }

    @Test
    void shouldSaveStateWhenFromExecute() {
        //when
        var state = new State("hello world!", 1);
        var testTaskModel = extendedTaskGenerator.generate(TestStatefulTaskModelSpec.builder(String.class, State.class)
            .withSaveInstance()
            .action((ctx, holder) -> {
                    holder.set(state);
                    throw new RuntimeException();
                }
            )
            .failureAction((ctx, holder) -> {
                    assertThat(holder.get()).isPresent().get().isEqualTo(state);
                    return false;
                }
            )
            .build()
        );

        //do
        getTaskWorker().execute(testTaskModel.getTaskEntity(), testTaskModel.getRegisteredTask());

        //verify
        verifyTaskState(Objects.requireNonNull(testTaskModel.getTaskId()), State.class, state);
    }

    @Test
    void shouldSaveStateWhenFromOnFailure() {
        //when
        var state = new State("hello world!", 1);
        var testTaskModel = extendedTaskGenerator.generate(TestStatefulTaskModelSpec.builder(String.class, State.class)
            .withSaveInstance()
            .action((ctx, holder) -> {
                    throw new RuntimeException();
                }
            )
            .failureAction((ctx, holder) -> {
                    holder.set(state);
                    return false;
                }
            )
            .build()
        );

        //do
        getTaskWorker().execute(testTaskModel.getTaskEntity(), testTaskModel.getRegisteredTask());

        //verify
        verifyTaskState(Objects.requireNonNull(testTaskModel.getTaskId()), State.class, state);
    }

    @Test
    void shouldAccessAndRewriteStateWhenFromExecute() {
        //when
        var state = new State("hello world!", 1);
        var state2 = new State("hello world!", 2);
        var testTaskModel = extendedTaskGenerator.generate(TestStatefulTaskModelSpec.builder(String.class, State.class)
            .withSaveInstance()
            .withLocalState(state)
            .action((ctx, holder) -> {
                    assertThat(holder.get()).isPresent().get().isEqualTo(state);
                    holder.set(state2);
                    throw new RuntimeException();
                }
            )
            .failureAction((ctx, holder) -> {
                    assertThat(holder.get()).isPresent().get().isEqualTo(state2);
                    return false;
                }
            )
            .build()
        );

        //do
        getTaskWorker().execute(testTaskModel.getTaskEntity(), testTaskModel.getRegisteredTask());

        //verify
        verifyTaskState(Objects.requireNonNull(testTaskModel.getTaskId()), State.class, state2);
    }

    @Test
    void shouldAccessAndRewriteStateWhenFromOnFailure() {
        //when
        var state = new State("hello world!", 1);
        var state2 = new State("hello world!", 2);
        var testTaskModel = extendedTaskGenerator.generate(TestStatefulTaskModelSpec.builder(String.class, State.class)
            .withSaveInstance()
            .withLocalState(state)
            .action((ctx, holder) -> {
                    throw new RuntimeException();
                }
            )
            .failureAction((ctx, holder) -> {
                    assertThat(holder.get()).isPresent().get().isEqualTo(state);
                    holder.set(state2);
                    return false;
                }
            )
            .build()
        );

        //do
        getTaskWorker().execute(testTaskModel.getTaskEntity(), testTaskModel.getRegisteredTask());

        //verify
        verifyTaskState(Objects.requireNonNull(testTaskModel.getTaskId()), State.class, state2);
    }
}
