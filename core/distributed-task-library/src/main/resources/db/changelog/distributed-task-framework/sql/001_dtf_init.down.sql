ALTER TABLE "_____dtf_remote_task_worker_table"
    DROP CONSTRAINT IF EXISTS "fk_remote_task_worker_table_node_state";
ALTER TABLE "_____dtf_registered_tasks"
    DROP CONSTRAINT IF EXISTS "fk_node_state";
ALTER TABLE "_____dtf_planner_table"
    DROP CONSTRAINT IF EXISTS "fk_node_state";
ALTER TABLE "_____dtf_capabilities"
    DROP CONSTRAINT IF EXISTS "_____dtf_capabilities_fk_node_state";
DROP TABLE IF EXISTS _____dtf_registered_tasks;
DROP TABLE IF EXISTS _____dtf_dlc_commands;
DROP TABLE IF EXISTS _____dtf_capabilities;
DROP INDEX IF EXISTS _____dtf_tasks_da_vq_idx;
DROP INDEX IF EXISTS _____dtf_tasks_tn_afg_vq_edu_idx;
DROP INDEX IF EXISTS _____dtf_tasks_vq_ag_wcdu_idx;
DROP INDEX IF EXISTS _____dtf_tasks_ag_a_vq_wid_idx;
DROP INDEX IF EXISTS _____dtf_tasks_aw_idx;
DROP INDEX IF EXISTS _____dtf_tasks_s_idx;
DROP INDEX IF EXISTS _____dtf_partitions_afg_tn_tb_idx;
DROP INDEX IF EXISTS _____dtf_join_task_message_table_ttji_jti_idx;
DROP INDEX IF EXISTS _____dtf_join_task_message_table_jti_idx;
DROP INDEX IF EXISTS _____dtf_join_task_link_table_jti_c_idx;
DROP INDEX IF EXISTS _____dtf_join_task_link_table_ttji_jti_idx;
DROP INDEX IF EXISTS _____dtf_remote_task_worker_table_an_idx;
DROP INDEX IF EXISTS _____dtf_remote_commands_an_tn_idx;
DROP INDEX IF EXISTS _____dtf_remote_commands_d_cd_idx;
DROP INDEX IF EXISTS _____dtf_planner_table_gn_idx;
DROP INDEX IF EXISTS _____dtf_node_state_ludu_idx;
DROP INDEX IF EXISTS _____dtf_tasks_vq_cdu_idx;
DROP TABLE IF EXISTS _____dtf_tasks;
DROP TYPE IF EXISTS _____dtf_virtual_queue_type;
DROP TABLE IF EXISTS _____dtf_partitions;
DROP TABLE IF EXISTS _____dtf_join_task_message_table;
DROP TABLE IF EXISTS _____dtf_join_task_link_table;
DROP TABLE IF EXISTS _____dtf_remote_task_worker_table;
DROP TABLE IF EXISTS _____dtf_remote_commands;
DROP TABLE IF EXISTS _____dtf_dlt_tasks;
DROP TABLE IF EXISTS _____dtf_planner_table;
DROP TABLE IF EXISTS _____dtf_node_state;