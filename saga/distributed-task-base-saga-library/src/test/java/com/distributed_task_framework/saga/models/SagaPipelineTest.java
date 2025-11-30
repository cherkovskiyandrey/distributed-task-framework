package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.saga.exceptions.SagaOutBoundException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SagaPipelineTest {

    @Nested
    class HasNextTest {

        @Test
        void shouldHasNextWhenEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isFalse();
        }

        @Test
        void shouldHasNextWhenOneElementForward() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.rewind();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isTrue();
        }

        @Test
        void shouldNotHasNextWhenBackwardAndEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isFalse();
        }

        @Test
        void shouldHasNextWhenBackwardAndOnlyOne() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isTrue();
        }

        @Test
        void shouldNotHasNextWhenBackwardAndNotEmptyButEmptyRevertOperations() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isFalse();
        }

        @Test
        void shouldHasNextWhenBackwardAndHasFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.hasNext()).isTrue();
        }

        @Test
        void shouldHasNextWhenBackwardAndHasNotFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
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
            var sagaEmbeddedPipelineContext = new SagaPipeline();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::moveToNext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldMoveToNextWhenOneElementForward() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            var sagaAction = emptySaga();
            sagaEmbeddedPipelineContext.addAction(sagaAction);
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewind();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentAction())
                .matches(actualSagaContext -> actualSagaContext == sagaAction);
        }

        @Test
        void shouldThrowExceptionWhenBackwardAndEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::moveToNext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldMoveToNextWhenBackwardAndOnlyOne() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            var revertSagaAction = withRevertSaga();
            sagaEmbeddedPipelineContext.addAction(revertSagaAction);
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentAction())
                .matches(actualSagaContext -> actualSagaContext == revertSagaAction);
        }

        @Test
        void shouldThrowExceptionWhenBackwardAndNotEmptyButEmptyRevertOperations() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::moveToNext).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldMoveToNextWhenBackwardAndHasFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            var revertSagaAction = withRevertSaga();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(revertSagaAction);
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentAction())
                .matches(actualSagaContext -> actualSagaContext == revertSagaAction);
        }

        @Test
        void shouldMoveToNextWhenBackwardAndHasNotFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            var revertSagaAction = withRevertSaga();
            sagaEmbeddedPipelineContext.addAction(revertSagaAction);
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do
            sagaEmbeddedPipelineContext.moveToNext();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentAction())
                .matches(actualSagaContext -> actualSagaContext == revertSagaAction);
        }
    }

    @Nested
    class RewindToRevertFromCurrentPositionTest {

        @Test
        void shouldSetTo0WhenBackwardAndEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(0);
        }

        @Test
        void shouldSetBefore0WhenBackwardAndOnlyOne() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(1);
        }

        @Test
        void shouldSetTo0WhenBackwardAndNotEmptyButEmptyRevertOperations() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(0);
        }

        @Test
        void shouldSetBeforeFirstWhenBackwardAndHasLastRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(3);
        }

        @Test
        void shouldSetBeforeFirstRevertWhenBackwardAndHasFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromCurrentPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(1);
        }

        @Test
        void shouldBeIdempotent() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
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
    class RewindToRevertFromPrevPositionTest {

        @Test
        void shouldSetTo0WhenBackwardAndEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(0);
        }

        @Test
        void shouldSetTo0WhenBackwardAndOnlyOne() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();

            //verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(0);
        }

        @Test
        void shouldSetTo0WhenBackwardAndNotEmptyButEmptyRevertOperations() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(0);
        }

        @Test
        void shouldSetBeforePrevWhenBackwardAndHasTwoLastRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(2);
        }

        @Test
        void shouldSetBeforeFirstRevertWhenBackwardAndHasFirstRevertOperation() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(1);
        }

        @Test
        void shouldBeIdempotent() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.addAction(withRevertSaga());
            sagaEmbeddedPipelineContext.moveToEnd();

            //do
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();
            sagaEmbeddedPipelineContext.rewindToRevertFromPrevPosition();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCursor()).isEqualTo(2);
        }
    }

    @Nested
    class GetCurrentSagaTest {

        @Test
        void shouldReturnElementsFromCursorWhenGetCurrentSagaContext() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            var sagaContext = emptySaga();
            sagaEmbeddedPipelineContext.addAction(sagaContext);
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getCurrentAction())
                .matches(actualSagaContext -> actualSagaContext == sagaContext);
        }

        @Test
        void shouldThrowExceptionWhenGetCurrentSagaContextAndCursorBeforeFirstElement() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getCurrentAction).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldThrowExceptionWhenGetCurrentSagaContext() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getCurrentAction).isInstanceOf(SagaOutBoundException.class);
        }
    }

    @Nested
    class GetRootSagaTest {

        @Test
        void shouldThrowExceptionWhenEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getRootAction).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldReturnRootContext() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            var sagaContext = emptySaga();
            sagaEmbeddedPipelineContext.addAction(sagaContext);
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getRootAction())
                .matches(actualSagaContext -> actualSagaContext == sagaContext);
        }
    }

    @Nested
    class GetParentSagaTest {

        @Test
        void shouldThrowExceptionWhenEmpty() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();

            //do & verify
            assertThatThrownBy(sagaEmbeddedPipelineContext::getParentAction).isInstanceOf(SagaOutBoundException.class);
        }

        @Test
        void shouldReturnEmptyWhenFromFirstElement() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getParentAction()).isEmpty();
        }

        @Test
        void shouldReturnParentWhenFromNotFirstElement() {
            //when
            var sagaEmbeddedPipelineContext = new SagaPipeline();
            var sagaElement = emptySaga();
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.addAction(sagaElement);
            sagaEmbeddedPipelineContext.addAction(emptySaga());
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();
            sagaEmbeddedPipelineContext.moveToNext();

            //do & verify
            assertThat(sagaEmbeddedPipelineContext.getParentAction())
                .isPresent()
                .get()
                .matches(actualSagaContext -> actualSagaContext == sagaElement);
        }
    }

    private SagaAction emptySaga() {
        return SagaAction.builder().build();
    }

    private SagaAction withRevertSaga() {
        return SagaAction.builder()
            .sagaRevertMethodTaskName("revert")
            .build();
    }

}