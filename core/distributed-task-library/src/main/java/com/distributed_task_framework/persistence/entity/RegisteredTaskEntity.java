package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.util.UUID;

@Table("_____dtf_registered_tasks")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class RegisteredTaskEntity {
    @Id
    @EqualsAndHashCode.Exclude
    UUID id;

    //foreign key to NodeStateEntity#node
    UUID nodeStateId;
    String taskName;
}
