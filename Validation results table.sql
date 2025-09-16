-- ==================================================================
-- VALIDATION RESULTS TABLE
-- ==================================================================
CREATE OR REPLACE TABLE DATABASE_NAME.SCHEMA_NAME.DO_RESULTS_TEST (
    TABLE_NAME   VARCHAR(1000),
    SCHEMA_NAME  VARCHAR(1000),
    CHECK_TYPE   VARCHAR(1000),
    STATUS       VARCHAR(1000),
    DETAILS      VARCHAR(10000),
    RUN_TIMESTAMP TIMESTAMP_NTZ(9)
);