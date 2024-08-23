package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_remote_commands")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class RemoteCommandEntity {
    @Id
    UUID id;
    String action;
    String appName;
    String taskName;
    @CreatedDate
    LocalDateTime createdDateUtc;
    LocalDateTime sendDateUtc;
    @ToString.Exclude
    byte[] body;
    int failures;
}
