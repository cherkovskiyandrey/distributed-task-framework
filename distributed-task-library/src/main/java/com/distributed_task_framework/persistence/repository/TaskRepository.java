package com.distributed_task_framework.persistence.repository;

import com.distributed_task_framework.persistence.entity.TaskEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * Current active indexes:
 * - _____dtf_tasks_pkey
 * - _____dtf_tasks_aw_idx
 * - _____dtf_tasks_s_idx (old name: tasks_name_singleton_index)
 * - _____dtf_tasks_tn_afg_vq_edu_idx
 * - _____dtf_tasks_vq_cdu_idx
 * - _____dtf_tasks_vq_ag_wcdu_idx
 * - _____dtf_tasks_ag_a_vq_idx
 *
 * Current active but will be deprecated after VQB showed stability and don't need to rollback to previous planner
 * - tasks_planner_index (@Deprecated) (draft-db.changelog-vqb-clean-indexes.yaml)
 *
 *
 * Deprecated and removed:
 * - tasks_assigned_worker_id_index
 * - affinity_group_affinity_workflow_created_date_utc_workflow_id_i
 * - assigned_worker_execution_date_utc_workflow_id_task_name_index
 * - tasks_task_name_version_index
 */
@Repository
public interface TaskRepository extends
        CrudRepository<TaskEntity, UUID>,
        TaskExtendedRepository,
        TaskCommandRepository,
        TaskVirtualQueueBasePlannerRepository,
        VirtualQueueManagerPlannerRepository,
        PartitionTrackerRepository,
        TaskWorkerRepository,
        TaskStatRepository {
}
