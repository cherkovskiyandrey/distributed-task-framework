package com.distributed_task_framework.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import jakarta.annotation.Nullable;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AffinityGroupStat {
    @Nullable
    String affinityGroupName;
    Integer number;

    public void increaseNumber() {
        number = number + 1;
    }

    public void decreaseNumber() {
        number = number - 1;
    }
}
