package com.distributed_task_framework.comparator;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;

/**
 * Rounds LocalDateTime to microseconds.
 */
public class RoundingLocalDateTimeComparator implements Comparator<LocalDateTime> {
    @Override
    public int compare(LocalDateTime o1, LocalDateTime o2) {
        if (o1 == null && o2 == null)
            return 0;

        if (o1 == null)
            return 1;

        if (o2 == null)
            return -1;

        return roundToMicroseconds(o1).compareTo(roundToMicroseconds(o2));
    }

    private LocalDateTime roundToMicroseconds(LocalDateTime dateTime) {
        final int nano = dateTime.getNano();
        final int roundedNano = nano % 1000 >= 500
                ? (nano / 1000 + 1) * 1000
                : (nano / 1000) * 1000;

        final int max = (int) ChronoField.NANO_OF_SECOND.range().getMaximum();
        if (roundedNano > max) {
            // Increment second and set nanos to 0
            return dateTime.truncatedTo(ChronoUnit.SECONDS)
                    .plusSeconds(1);
        }

        return dateTime.withNano(roundedNano);

    }
}
