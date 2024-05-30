package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.experimental.FieldNameConstants;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.UUID;

@Table("_____dtf_partitions")
@FieldNameConstants
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class PartitionEntity {
    public static final Comparator<PartitionEntity> COMPARATOR = Comparator
            .comparing(PartitionEntity::getAffinityGroup, Comparator.nullsFirst(String::compareTo))
            .thenComparing(PartitionEntity::getTaskName, Comparator.nullsFirst(String::compareTo))
            .thenComparing(PartitionEntity::getTimeBucket, Comparator.nullsFirst(Long::compareTo));

    @Id
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    UUID id;
    @Nullable
    String affinityGroup;
    String taskName;
    Long timeBucket;
}
