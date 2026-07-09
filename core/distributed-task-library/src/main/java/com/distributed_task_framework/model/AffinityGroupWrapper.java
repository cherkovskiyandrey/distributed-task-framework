package com.distributed_task_framework.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import jakarta.annotation.Nullable;
import lombok.experimental.FieldNameConstants;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@FieldNameConstants
@Builder
public class AffinityGroupWrapper {
    public static final AffinityGroupWrapper EMPTY = new AffinityGroupWrapper(null);
    @Nullable
    String affinityGroup;
}
