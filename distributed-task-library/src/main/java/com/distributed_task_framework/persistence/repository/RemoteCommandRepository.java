package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.RemoteCommandEntity;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.UUID;

@Repository
public interface RemoteCommandRepository extends CrudRepository<RemoteCommandEntity, UUID> {

    @Modifying
    void deleteAllByAppNameAndTaskName(String appName, String taskName);

    @Query("""
            SELECT * FROM _____dtf_remote_commands
            WHERE app_name = :appName
            AND send_date_utc <= :sendDateUtc 
            ORDER BY created_date_utc 
            LIMIT :limit
            """)
    Collection<RemoteCommandEntity> findCommandsToSend(String appName, LocalDateTime sendDateUtc, long limit);
}
