package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.saga.exceptions.SagaOutBoundException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaEmbeddedPipelineContextTest {

    @Nested
    class HasNextTest {

        @Test
        void shouldHasNextWhenEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isFalse();
        }

        @Test
        void shouldHasNextWhenOneElementForward() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.rewind();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isTrue();
        }

        @Test
        void shouldNotHasNextWhenBackwardAndEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isFalse();
        }

        @Test
        void shouldHasNextWhenBackwardAndOnlyOne() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isTrue();
        }

        @Test
        void shouldNotHasNextWhenBackwardAndNotEmptyButEmptyRevertOperations() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isFalse();
        }

        @Test
        void shouldHasNextWhenBackwardAndHasFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isTrue();
        }

        @Test
        void shouldHasNextWhenBackwardAndHasNotFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(withRevertSaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isTrue();
        }
    }

    @Nested
    class MoveToNextTest {

        @Test
        void shouldThrowExceptionWhenEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::moveToNext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldMoveToNextWhenOneElementForward() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            var sagaAction = emptySaga();
            sagaEmbeddedPipelineContext.addSagaContext(sagaAction);
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewind();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentSagaContext())
                .matches(actualSagaContext -> actualSagaContext == sagaAction);
        }

        @Test
        void shouldThrowExceptionWhenBackwardAndEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::moveToNext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldMoveToNextWhenBackwardAndOnlyOne() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            var revertSagaAction = withRevertSaga();
            sagaEmbeddedPipelineContext.addSagaContext(revertSagaAction);
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentSagaContext())
                .matches(actualSagaContext -> actualSagaContext == revertSagaAction);
        }

        @Test
        void shouldThrowExceptionWhenBackwardAndNotEmptyButEmptyRevertOperations() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::moveToNext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldMoveToNextWhenBackwardAndHasFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            var revertSagaAction = withRevertSaga();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(revertSagaAction);
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentSagaContext())
                .matches(actualSagaContext -> actualSagaContext == revertSagaAction);
        }

        @Test
        void shouldMoveToNextWhenBackwardAndHasNotFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            var revertSagaAction = withRevertSaga();
            sagaEmbeddedPipelineContext.addSagaContext(revertSagaAction);
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentSagaContext())
                .matches(actualSagaContext -> actualSagaContext == revertSagaAction);
        }
    }

    @Nested
    class RewindToRevertFromCurrentPositionTest {

        @Test
        void shouldSetTo0WhenBackwardAndEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(0);
        }

        @Test
        void shouldSetBefore0WhenBackwardAndOnlyOne() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(1);
        }

        @Test
        void shouldSetTo0WhenBackwardAndNotEmptyButEmptyRevertOperations() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(0);
        }

        @Test
        void shouldSetBeforeFirstWhenBackwardAndHasLastRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(3);
        }

        @Test
        void shouldSetBeforeFirstRevertWhenBackwardAndHasFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(withRevertSaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(1);
        }

        @Test
        void shouldBeIdempotent() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(3);
        }
    }

    @Nested
    class GetCurrentSagaContextTest {

        @Test
        void shouldReturnElementsFromCursorWhenGetCurrentSagaContext() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            var sagaContext = emptySaga();
            sagaEmbeddedPipelineContext.addSagaContext(sagaContext);
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentSagaContext())
                .matches(actualSagaContext -> actualSagaContext == sagaContext);
        }

        @Test
        void shouldThrowExceptionWhenGetCurrentSagaContextAndCursorBeforeFirstElement() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getCurrentSagaContext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldThrowExceptionWhenGetCurrentSagaContext() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getCurrentSagaContext).isInstanceOf(SagaOutBoundException.class);
        }
    }

    @Nested
    class GetRootSagaContextTest {

        @Test
        void shouldThrowExceptionWhenEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getRootSagaContext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldReturnRootContext() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            var sagaContext = emptySaga();
            sagaEmbeddedPipelineContext.addSagaContext(sagaContext);
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getRootSagaContext())
                .matches(actualSagaContext -> actualSagaContext == sagaContext);
        }
    }

    @Nested
    class GetParentSagaContextTest {

        @Test
        void shouldThrowExceptionWhenEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getParentSagaContext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldReturnEmptyWhenFromFirstElement() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getParentSagaContext()).isEmpty();
        }

        @Test
        void shouldReturnParentWhenFromNotFirstElement() {
            //when
            var sagaEmbeddedPipelineContext = new SagaEmbeddedPipelineContext();
            var sagaElement = emptySaga();
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.addSagaContext(sagaElement);
            sagaEmbeddedPipelineContext.addSagaContext(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getParentSagaContext())
                .isPresent()
                .get()
                .matches(actualSagaContext -> actualSagaContext == sagaElement);
        }
    }

    private SagaEmbeddedActionContext emptySaga() {
        return SagaEmbeddedActionContext.builder().build();
    }

    private SagaEmbeddedActionContext withRevertSaga() {
        return SagaEmbeddedActionContext.builder()
            .sagaRevertMethodTaskName("revert")
            .build();
    }

}