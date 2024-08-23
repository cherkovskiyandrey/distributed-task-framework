CREATE TABLE "_____dtf_saga_context"
(
    "saga_id"            UUID                        NOT NULL,
    "created_date_utc"   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "completed_date_utc" TIMESTAMP WITHOUT TIME ZONE,
    "root_task_id"       BYTEA                       NOT NULL,
    "exception_type"     VARCHAR(255),
    "result"             BYTEA,
    CONSTRAINT "_____dtf_saga_context_pkey" PRIMARY KEY ("saga_id")
);