package com.distributed_task_framework.persistence.entity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Table("_____dtf_dlc_commands")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class DlcEntity implements Persistable<UUID> {
    @Id
    UUID id;
    String action;
    String appName;
    String taskName;
    LocalDateTime createdDateUtc;
    byte[] body;

    @Override
    public boolean isNew() {
        return true;
    }
}
