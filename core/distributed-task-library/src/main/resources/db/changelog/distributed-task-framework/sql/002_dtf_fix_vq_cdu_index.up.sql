DROP INDEX "_____dtf_tasks_vq_cdu_idx";
CREATE INDEX "_____dtf_tasks_vq_cdu_idx" ON "_____dtf_tasks" ("virtual_queue", "created_date_utc");