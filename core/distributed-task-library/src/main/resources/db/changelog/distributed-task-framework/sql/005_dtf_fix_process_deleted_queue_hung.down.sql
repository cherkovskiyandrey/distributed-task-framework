DROP INDEX IF EXISTS _____dtf_tasks_aw_idx;
CREATE INDEX "_____dtf_tasks_aw_idx" ON "_____dtf_tasks" ("assigned_worker");

DROP INDEX IF EXISTS _____dtf_tasks_new_afg_wcdu_idx;
CREATE INDEX "_____dtf_tasks_vq_cdu_idx" ON "_____dtf_tasks" ("created_date_utc");
CREATE INDEX "_____dtf_tasks_vq_ag_wcdu_idx" ON "_____dtf_tasks" ("virtual_queue", "affinity_group", "workflow_created_date_utc");

CREATE INDEX "_____dtf_tasks_da_vq_idx" ON "_____dtf_tasks" ("deleted_at", "virtual_queue");
DROP INDEX IF EXISTS "_____dtf_tasks_delete_purge_idx";
DROP INDEX IF EXISTS "_____dtf_partitions_tb_afg_tn_id_idx";
DROP INDEX IF EXISTS "_____dtf_tasks_parked_ag_a_wcdu_wid_idx";