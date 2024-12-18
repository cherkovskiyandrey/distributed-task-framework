package com.distributed_task_framework.saga.models;

import com.distributed_task_framework.saga.exceptions.SagaOutBoundException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Getter
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class SagaPipeline {
    final UUID sagaId;
    final List<SagaAction> sagaActions;
    int cursor;
    boolean forward;

    public SagaPipeline() {
        this.sagaId = UUID.randomUUID();
        this.sagaActions = Lists.newArrayList();
        this.cursor = -1;
        this.forward = true;
    }

    /**
     * Set direction to forward and move cursor to BEFORE begin.
     */
    public void rewind() {
        forward = true;
        cursor = -1;
    }

    /**
     * Set direction to backward and move cursor to BEFORE the first valid revert operation from current one.
     */
    public void rewindToRevertFromCurrentPosition() {
        rewindToRevertFromPosition(cursor + 1);
    }

    /**
     * Set direction to backward and move cursor to BEFORE the first valid revert operation from previous one.
     */
    public void rewindToRevertFromPrevPosition() {
        rewindToRevertFromPosition(cursor);
    }

    private void rewindToRevertFromPosition(int position) {
        forward = false;
        cursor = Math.max(Math.min(position, sagaActions.size()), 0);
        for (; cursor > 0; cursor -= 1) {
            if (hasValidRevertOperation(cursor - 1)) {
                return;
            }
        }
    }

    /**
     * Check whether exist SagaContext on next position?
     * Works for both directions.
     *
     * @return
     */
    @JsonIgnore
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean hasNext() {
        if (forward) {
            return cursor + 1 < sagaActions.size();
        }

        for (var revCursor = cursor; revCursor > 0; revCursor -= 1) {
            if (hasValidRevertOperation(revCursor - 1)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Move cursor to one position.
     * Works for both directions.
     */
    public void moveToNext() {
        if (forward) {
            cursor += 1;
            checkBorders();
            return;
        }

        while ((cursor -= 1) >= 0) {
            if (hasValidRevertOperation(cursor)) {
                return;
            }
        }
        checkBorders();
    }

    /**
     * Move cursor to the end and point to the last or first element.
     * Work for both directions.
     */
    @VisibleForTesting
    void moveToEnd() {
        if (sagaActions.isEmpty()) {
            return;
        }

        if (forward) {
            cursor = sagaActions.size() - 1;
        } else {
            cursor = 0;
        }
    }

    /**
     * Return SagaAction on current position.
     *
     * @return
     */
    @JsonIgnore
    public SagaAction getCurrentAction() {
        checkBorders();
        return sagaActions.get(cursor);
    }

    @JsonIgnore
    public SagaAction getRootAction() {
        checkBorders();
        return sagaActions.get(0);
    }

    /**
     * Get parent SagaAction if exists.
     * Always return parent from forward standpoint.
     *
     * @return
     */
    @JsonIgnore
    public Optional<SagaAction> getParentAction() {
        checkBorders();
        if (cursor - 1 >= 0) {
            return Optional.ofNullable(sagaActions.get(cursor - 1));
        }
        return Optional.empty();
    }

    /**
     * Add SagaAction to next position.
     * Always add to forward direction.
     *
     * @param sagaAction
     */
    @JsonIgnore
    public void addAction(SagaAction sagaAction) {
        if (!forward) {
            throw new UnsupportedOperationException("Isn't supported");
        }
        sagaActions.add(sagaAction);
    }

    /**
     * Set SagaAction on current position.
     *
     * @param sagaAction
     */
    @JsonIgnore
    public void setCurrentAction(SagaAction sagaAction) {
        checkBorders();
        sagaActions.set(cursor, sagaAction);
    }

    private void checkBorders() {
        if (sagaActions.isEmpty() || cursor < 0 || cursor >= sagaActions.size()) {
            throw new SagaOutBoundException(
                "checkBorders(): rawCursor=[%d], sagaContexts.size=[%d]".formatted(cursor, sagaActions.size())
            );
        }
    }

    @JsonIgnore
    private boolean hasValidRevertOperation(int cursor) {
        var sagaContext = sagaActions.get(cursor);
        return sagaContext != null && sagaContext.getSagaRevertMethodTaskName() != null;
    }
}
