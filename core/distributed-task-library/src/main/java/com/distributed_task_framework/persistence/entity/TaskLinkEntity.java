package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.util.UUID;

@Table("_____dtf_join_task_link_table")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class TaskLinkEntity {
    @Id
    UUID id;
    UUID taskToJoinId;
    UUID joinTaskId;
    String joinTaskName;
    boolean completed;
}
