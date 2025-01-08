CREATE TABLE "_____dtf_node_state"
(
    "node"                 UUID                        NOT NULL,
    "version"              INTEGER                     NOT NULL,
    "last_update_date_utc" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "median_cpu_loading"   FLOAT8,
    CONSTRAINT "node_state_pkey" PRIMARY KEY ("node")
);
CREATE TABLE "_____dtf_planner_table"
(
    "id"            UUID                           NOT NULL,
    "group_name"    VARCHAR(255) DEFAULT 'default' NOT NULL,
    "node_state_id" UUID                           NOT NULL,
    CONSTRAINT "scheduler_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_dlt_tasks"
(
    "id"                     UUID                        NOT NULL,
    "task_name"              VARCHAR(255)                NOT NULL,
    "version"                INTEGER                     NOT NULL,
    "workflow_id"            UUID,
    "created_date_utc"       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "last_assigned_date_utc" TIMESTAMP WITHOUT TIME ZONE,
    "execution_date_utc"     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "message_bytes"          BYTEA,
    "failures"               INTEGER DEFAULT 0           NOT NULL,
    CONSTRAINT "dlt_tasks_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_remote_commands"
(
    "id"               UUID                        NOT NULL,
    "action"           VARCHAR(255)                NOT NULL,
    "app_name"         VARCHAR(255)                NOT NULL,
    "task_name"        VARCHAR(255)                NOT NULL,
    "created_date_utc" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "send_date_utc"    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "body"             BYTEA,
    "failures"         INTEGER DEFAULT 0           NOT NULL,
    CONSTRAINT "remote_commands_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_remote_task_worker_table"
(
    "id"            UUID         NOT NULL,
    "app_name"      VARCHAR(255) NOT NULL,
    "node_state_id" UUID         NOT NULL,
    CONSTRAINT "remote_task_worker_table_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_join_task_link_table"
(
    "id"              UUID                  NOT NULL,
    "task_to_join_id" UUID                  NOT NULL,
    "join_task_id"    UUID                  NOT NULL,
    "join_task_name"  VARCHAR(255)          NOT NULL,
    "completed"       BOOLEAN DEFAULT FALSE NOT NULL,
    CONSTRAINT "_____dtf_join_task_link_table_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_join_task_message_table"
(
    "id"              UUID NOT NULL,
    "task_to_join_id" UUID NOT NULL,
    "join_task_id"    UUID NOT NULL,
    "message"         BYTEA,
    CONSTRAINT "_____dtf_join_task_message_table_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_partitions"
(
    "id"             UUID         NOT NULL,
    "affinity_group" VARCHAR(255),
    "task_name"      VARCHAR(255) NOT NULL,
    "time_bucket"    INTEGER      NOT NULL,
    CONSTRAINT "_____dtf_partitions_pk" PRIMARY KEY ("id")
);
CREATE TYPE _____dtf_virtual_queue_type AS enum ('NEW', 'READY', 'PARKED', 'DELETED');
CREATE TABLE "_____dtf_tasks"
(
    "id"                        UUID                             NOT NULL,
    "task_name"                 VARCHAR(255)                     NOT NULL,
    "workflow_id"               UUID                             NOT NULL,
    "version"                   INTEGER                          NOT NULL,
    "created_date_utc"          TIMESTAMP WITHOUT TIME ZONE      NOT NULL,
    "assigned_worker"           UUID,
    "last_assigned_date_utc"    TIMESTAMP WITHOUT TIME ZONE,
    "execution_date_utc"        TIMESTAMP WITHOUT TIME ZONE      NOT NULL,
    "singleton"                 BOOLEAN            DEFAULT FALSE NOT NULL,
    "message_bytes"             BYTEA,
    "failures"                  INTEGER            DEFAULT 0     NOT NULL,
    "affinity"                  VARCHAR(255),
    "affinity_group"            VARCHAR(255),
    "workflow_created_date_utc" TIMESTAMP WITHOUT TIME ZONE      NOT NULL,
    "not_to_plan"               BOOLEAN            DEFAULT FALSE NOT NULL,
    "canceled"                  BOOLEAN            DEFAULT FALSE NOT NULL,
    "join_message_bytes"        BYTEA,
    "virtual_queue"             _____DTF_VIRTUAL_QUEUE_TYPE DEFAULT 'NEW' NOT NULL,
    "deleted_at"                TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT "_____dtf_tasks_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "_____dtf_tasks_vq_cdu_idx" ON "_____dtf_tasks" ("created_date_utc");
CREATE INDEX "_____dtf_node_state_ludu_idx" ON "_____dtf_node_state" ("last_update_date_utc");
CREATE UNIQUE INDEX "_____dtf_planner_table_gn_idx" ON "_____dtf_planner_table" ("group_name");
CREATE INDEX "_____dtf_remote_commands_d_cd_idx" ON "_____dtf_remote_commands" ("app_name", "created_date_utc");
CREATE INDEX "_____dtf_remote_commands_an_tn_idx" ON "_____dtf_remote_commands" ("app_name", "task_name");
CREATE INDEX "_____dtf_remote_task_worker_table_an_idx" ON "_____dtf_remote_task_worker_table" ("app_name");
CREATE UNIQUE INDEX "_____dtf_join_task_link_table_ttji_jti_idx" ON "_____dtf_join_task_link_table" ("task_to_join_id", "join_task_id");
CREATE INDEX "_____dtf_join_task_link_table_jti_c_idx" ON "_____dtf_join_task_link_table" ("join_task_id", "completed");
CREATE INDEX "_____dtf_join_task_message_table_jti_idx" ON "_____dtf_join_task_message_table" ("join_task_id");
CREATE UNIQUE INDEX "_____dtf_join_task_message_table_ttji_jti_idx" ON "_____dtf_join_task_message_table" ("task_to_join_id", "join_task_id");
CREATE INDEX "_____dtf_partitions_afg_tn_tb_idx" ON "_____dtf_partitions" ("affinity_group", "task_name", "time_bucket");
CREATE UNIQUE INDEX "_____dtf_tasks_s_idx" ON "_____dtf_tasks" ("task_name", "singleton") WHERE singleton IS TRUE;
CREATE INDEX "_____dtf_tasks_aw_idx" ON "_____dtf_tasks" ("assigned_worker");
CREATE INDEX "_____dtf_tasks_ag_a_vq_wid_idx" ON "_____dtf_tasks" ("affinity_group", "affinity", "virtual_queue", "workflow_id");
CREATE INDEX "_____dtf_tasks_vq_ag_wcdu_idx" ON "_____dtf_tasks" ("virtual_queue", "affinity_group", "workflow_created_date_utc");
CREATE INDEX "_____dtf_tasks_tn_afg_vq_edu_idx" ON "_____dtf_tasks" ("task_name", "affinity_group", "virtual_queue", "execution_date_utc");
CREATE INDEX "_____dtf_tasks_da_vq_idx" ON "_____dtf_tasks" ("deleted_at", "virtual_queue");
CREATE TABLE "_____dtf_capabilities"
(
    "id"      UUID         NOT NULL,
    "node_id" UUID         NOT NULL,
    "value"   VARCHAR(255) NOT NULL,
    CONSTRAINT "_____dtf_capabilities_pk" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_dlc_commands"
(
    "id"               UUID                        NOT NULL,
    "action"           VARCHAR(255)                NOT NULL,
    "app_name"         VARCHAR(255)                NOT NULL,
    "task_name"        VARCHAR(255)                NOT NULL,
    "created_date_utc" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "body"             BYTEA,
    CONSTRAINT "dlc_commands_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "_____dtf_registered_tasks"
(
    "id"            UUID NOT NULL,
    "task_name"     VARCHAR(255),
    "node_state_id" UUID NOT NULL,
    CONSTRAINT "registered_tasks_pkey" PRIMARY KEY ("id")
);
ALTER TABLE "_____dtf_capabilities"
    ADD CONSTRAINT "_____dtf_capabilities_fk_node_state" FOREIGN KEY ("node_id") REFERENCES "_____dtf_node_state" ("node") ON UPDATE NO ACTION ON DELETE CASCADE;
ALTER TABLE "_____dtf_planner_table"
    ADD CONSTRAINT "fk_node_state" FOREIGN KEY ("node_state_id") REFERENCES "_____dtf_node_state" ("node") ON UPDATE NO ACTION ON DELETE CASCADE;
ALTER TABLE "_____dtf_registered_tasks"
    ADD CONSTRAINT "fk_node_state" FOREIGN KEY ("node_state_id") REFERENCES "_____dtf_node_state" ("node") ON UPDATE NO ACTION ON DELETE CASCADE;
ALTER TABLE "_____dtf_remote_task_worker_table"
    ADD CONSTRAINT "fk_remote_task_worker_table_node_state" FOREIGN KEY ("node_state_id") REFERENCES "_____dtf_node_state" ("node") ON UPDATE NO ACTION ON DELETE CASCADE;

