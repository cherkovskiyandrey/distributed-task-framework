CREATE INDEX "_____dtf_tasks_da_vq_idx" ON "_____dtf_tasks" ("deleted_at", "virtual_queue");
DROP INDEX IF EXISTS "_____dtf_tasks_delete_purge_idx";
DROP INDEX IF EXISTS "_____dtf_partitions_tb_afg_tn_id_idx";
DROP INDEX IF EXISTS "_____dtf_tasks_parked_ag_a_wcdu_wid_idx";