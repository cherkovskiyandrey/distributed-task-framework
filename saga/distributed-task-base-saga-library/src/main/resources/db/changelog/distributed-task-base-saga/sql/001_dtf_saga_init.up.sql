CREATE TABLE "_____dtf_saga"
(
    "saga_id"                                UUID                        NOT NULL,
    "name"                                   VARCHAR(255),
    "created_date_utc"                       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "completed_date_utc"                     TIMESTAMP WITHOUT TIME ZONE,
    "available_after_completion_timeout_sec" INTEGER,
    "stop_on_failed_any_revert"              BOOLEAN DEFAULT FALSE       NOT NULL,
    "expiration_date_utc"                    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "canceled"                               BOOLEAN DEFAULT FALSE       NOT NULL,
    "root_task_id"                           BYTEA                       NOT NULL,
    "exception_type"                         VARCHAR(255),
    "result"                                 BYTEA,
    "last_pipeline_context"                  BYTEA                       NOT NULL,
    CONSTRAINT "_____dtf_saga_pkey" PRIMARY KEY ("saga_id")
);
CREATE INDEX "cdu_idx" ON "_____dtf_saga" ("completed_date_utc");
CREATE INDEX "edu_cdu_idx" ON "_____dtf_saga" ("expiration_date_utc", "completed_date_utc");

CREATE TABLE "_____dtf_saga_dls"
(
    "saga_id"                                UUID                        NOT NULL,
    "name"                                   VARCHAR(255),
    "created_date_utc"                       TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "completed_date_utc"                     TIMESTAMP WITHOUT TIME ZONE,
    "available_after_completion_timeout_sec" INTEGER,
    "stop_on_failed_any_revert"              BOOLEAN DEFAULT FALSE       NOT NULL,
    "expiration_date_utc"                    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "root_task_id"                           BYTEA                       NOT NULL,
    "last_pipeline_context"                  BYTEA                       NOT NULL,
    CONSTRAINT "_____dtf_saga_dls_pkey" PRIMARY KEY ("saga_id")
);