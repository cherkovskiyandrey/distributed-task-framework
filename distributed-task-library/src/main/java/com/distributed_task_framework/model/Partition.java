package com.distributed_task_framework.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;


@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class Partition implements Comparable<Partition> {
    @Nullable
    String affinityGroup;
    String taskName;

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(Partition o) {
        return Objects.compare(
                this,
                o,
                Comparator.comparing(Partition::getAffinityGroup, Comparator.nullsFirst(String::compareTo))
                        .thenComparing(Partition::getTaskName, Comparator.nullsFirst(String::compareTo))
        );
    }
}
