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

@Table("_____dtf_planner_table")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class PlannerEntity {
    @Id
    UUID id;
    /**
     * Determined planner group.
     * Group is need to have different mutually exclusive planners and
     * to be responsible for certain planner job. Like general or join.
     */
    String groupName;
    /**
     * Foreign key to NodeStateEntity#node
     */
    UUID nodeStateId;
}
