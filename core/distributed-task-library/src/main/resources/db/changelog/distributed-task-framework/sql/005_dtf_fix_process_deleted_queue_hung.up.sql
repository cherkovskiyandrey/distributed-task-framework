CREATE INDEX _____dtf_tasks_parked_ag_a_wcdu_wid_idx
    ON _____dtf_tasks (affinity_group, affinity, workflow_created_date_utc, workflow_id)
    INCLUDE (id, version)
    WHERE virtual_queue = 'PARKED';

CREATE INDEX _____dtf_partitions_tb_afg_tn_id_idx
    ON _____dtf_partitions ("time_bucket", "affinity_group", "task_name", "id");

CREATE INDEX _____dtf_tasks_delete_purge_idx
    ON _____dtf_tasks (deleted_at)
    INCLUDE (id, version, affinity_group, affinity)
    WHERE virtual_queue = 'DELETED';

DROP INDEX _____dtf_tasks_da_vq_idx;