package com.distributed_task_framework.service;

import com.distributed_task_framework.model.DltTask;
import com.distributed_task_framework.model.TaskDef;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.Collection;

public interface LocalDltTaskCommandService {

    /**
     * Get task types in DLT.
     *
     * @return
     */
    Collection<TaskDef<?>> getDltTaskDefs();

    /**
     * Get list of tasks in the DLT
     *
     * @param taskDef
     * @param <T>
     * @return
     */
    <T> Page<DltTask<?>> getDltTasksByDef(TaskDef<T> taskDef, Pageable pageable);

    /**
     * Try to reschedule dlt tasks.
     *
     * @param dltTasks
     */
    void rescheduleDltTasks(Collection<DltTask<?>> dltTasks);

    //todo: remove dltTask
}
