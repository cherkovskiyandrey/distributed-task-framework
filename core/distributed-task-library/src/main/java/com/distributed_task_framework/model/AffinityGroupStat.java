package com.distributed_task_framework.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import jakarta.annotation.Nullable;
import lombok.experimental.FieldNameConstants;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@FieldNameConstants
@Builder
public class AffinityGroupStat {
    public static final AffinityGroupStat NULL_GROUP = AffinityGroupStat.builder().build();
    public static final BeanPropertyRowMapper<AffinityGroupStat> AFFINITY_GROUP_STAT_MAPPER =
        new BeanPropertyRowMapper<>(AffinityGroupStat.class);

    @Nullable
    String affinityGroup;
    @EqualsAndHashCode.Exclude
    Integer number;

    public void increaseNumber() {
        number = number + 1;
    }

    public void decreaseNumber() {
        number = number - 1;
    }
}
