CREATE TABLE "_____dtf_saga_result"
(
    "saga_id"            UUID                        NOT NULL,
    "created_date_utc"   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "completed_date_utc" TIMESTAMP WITHOUT TIME ZONE,
    "exception_type"     VARCHAR(255),
    "result"             BYTEA,
    CONSTRAINT "_____dtf_saga_result_pkey" PRIMARY KEY ("saga_id")
);