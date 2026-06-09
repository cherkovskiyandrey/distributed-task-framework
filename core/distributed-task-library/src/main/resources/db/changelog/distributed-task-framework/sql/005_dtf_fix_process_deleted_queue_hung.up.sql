CREATE INDEX _____dtf_tasks_parked_ag_a_wcdu_wid_idx
    ON _____dtf_tasks (affinity_group, affinity, workflow_created_date_utc, workflow_id)
    INCLUDE (id, version)
    WHERE virtual_queue = 'PARKED';