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

@Table("_____dtf_remote_task_worker_table")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class RemoteTaskWorkerEntity {
    @Id
    UUID id;
    /**
     * Foreign application name current node is responsible to delivery.
     */
    String appName;
    /**
     * Foreign key to NodeStateEntity#node
     */
    UUID nodeStateId;
}
