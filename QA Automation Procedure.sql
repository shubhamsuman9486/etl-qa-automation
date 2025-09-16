CREATE OR REPLACE PROCEDURE DATABASE_NAME.SCHEMA_NAME.RUN_DQ_CHECKS_TEST()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Truncate results before new run
    snowflake.createStatement({
        sqlText: `TRUNCATE TABLE DATABASE_NAME.SCHEMA_NAME.DQ_RESULTS_TEST`
    }).execute();

    // Fetch config
    var rs = snowflake.createStatement({
        sqlText: `SELECT STG_TABLE, INT_TABLE, HIST_TABLE, KEYS 
                  FROM DATABASE_NAME.SCHEMA_NAME.DQ_TABLE_CONFIG_TEST`
    }).execute();

    var tables = [];
    while (rs.next()) {
        tables.push({
            stg_table: rs.getColumnValue(1),
            int_table: rs.getColumnValue(2),
            hist_table: rs.getColumnValue(3),
            keys: JSON.parse(rs.getColumnValue(4))
        });
    }

    for (var i = 0; i < tables.length; i++) {
        var t = tables[i];
        var stgTable = t.stg_table;
        var intTable = t.int_table;
        var histTable = t.hist_table;
        var keys = t.keys;

        var keyExpr = keys.join(', ');
        var tableName = histTable.split('.')[2];
        var schemaName = histTable.split('.')[1];

        // ----------------------
        // COUNT CHECK
        // ----------------------
        var sql_count = `
            SELECT
              (SELECT COUNT(*) FROM ${stgTable}) AS STG_COUNT,
              (SELECT COUNT(*) FROM ${intTable}) AS INT_COUNT,
              (SELECT COUNT(*) FROM ${histTable} WHERE CURRENT_FLAG='Y') AS HIST_COUNT
        `;
        var rs_count = snowflake.createStatement({sqlText: sql_count}).execute();
        rs_count.next();
        var stg_count = rs_count.getColumnValue(1);
        var int_count = rs_count.getColumnValue(2);
        var hist_count = rs_count.getColumnValue(3);

        var status = (int_count == hist_count) ? 'PASS' : 'FAIL';

        snowflake.createStatement({
            sqlText: `INSERT INTO DATABASE_NAME.SCHEMA_NAME.DQ_RESULTS_TEST 
                      (TABLE_NAME, SCHEMA_NAME, CHECK_TYPE, STATUS, DETAILS, RUN_TIMESTAMP)
                      VALUES (?, ?, 'COUNT_CHECK', ?, ?, CURRENT_TIMESTAMP)`,
            binds: [tableName, schemaName, status, `STG=${stg_count}, INT=${int_count}, HIST=${hist_count}`]
        }).execute();

        // ----------------------
        // DUPLICATE CHECK
        // ----------------------
        var sql_dup_stg = `SELECT COUNT(*) FROM (
                              SELECT ${keyExpr}, COUNT(*) 
                              FROM ${stgTable} 
                              GROUP BY ${keyExpr} 
                              HAVING COUNT(*)>1
                           )`;
        var sql_dup_int = `SELECT COUNT(*) FROM (
                              SELECT ${keyExpr}, COUNT(*) 
                              FROM ${intTable} 
                              GROUP BY ${keyExpr} 
                              HAVING COUNT(*)>1
                           )`;
        var sql_dup_hist = `SELECT COUNT(*) FROM (
                               SELECT ${keyExpr}, COUNT(*) 
                               FROM ${histTable} 
                               WHERE CURRENT_FLAG='Y'
                               GROUP BY ${keyExpr} 
                               HAVING COUNT(*)>1
                            )`;

        var rs_dup_stg = snowflake.createStatement({sqlText: sql_dup_stg}).execute(); rs_dup_stg.next();
        var rs_dup_int = snowflake.createStatement({sqlText: sql_dup_int}).execute(); rs_dup_int.next();
        var rs_dup_hist = snowflake.createStatement({sqlText: sql_dup_hist}).execute(); rs_dup_hist.next();

        var d1 = rs_dup_stg.getColumnValue(1);
        var d2 = rs_dup_int.getColumnValue(1);
        var d3 = rs_dup_hist.getColumnValue(1);

        var status = (d1 == 0 && d2 == 0 && d3 == 0) ? 'PASS' : 'FAIL';

        snowflake.createStatement({
            sqlText: `INSERT INTO DATABASE_NAME.SCHEMA_NAME.DQ_RESULTS_TEST 
                      (TABLE_NAME, SCHEMA_NAME, CHECK_TYPE, STATUS, DETAILS, RUN_TIMESTAMP)
                      VALUES (?, ?, 'DUPLICATE_CHECK', ?, ?, CURRENT_TIMESTAMP)`,
            binds: [tableName, schemaName, status, `STG_DUP=${d1}, INT_DUP=${d2}, HIST_DUP=${d3}`]
        }).execute();

        // ----------------------
        // NULL CHECK
        // ----------------------
        var null_conditions = keys.map(k => `${k} IS NULL`).join(' OR ');

        var sql_null_stg = `SELECT COUNT(*) FROM ${stgTable} WHERE ${null_conditions}`;
        var sql_null_int = `SELECT COUNT(*) FROM ${intTable} WHERE ${null_conditions}`;
        var sql_null_hist = `SELECT COUNT(*) FROM ${histTable} WHERE ${null_conditions} AND CURRENT_FLAG='Y'`;

        var rs_null_stg = snowflake.createStatement({sqlText: sql_null_stg}).execute(); rs_null_stg.next();
        var rs_null_int = snowflake.createStatement({sqlText: sql_null_int}).execute(); rs_null_int.next();
        var rs_null_hist = snowflake.createStatement({sqlText: sql_null_hist}).execute(); rs_null_hist.next();

        var n1 = rs_null_stg.getColumnValue(1);
        var n2 = rs_null_int.getColumnValue(1);
        var n3 = rs_null_hist.getColumnValue(1);

        var status = (n1 == 0 && n2 == 0 && n3 == 0) ? 'PASS' : 'FAIL';

        snowflake.createStatement({
            sqlText: `INSERT INTO DATABASE_NAME.SCHEMA_NAME.DQ_RESULTS_TEST 
                      (TABLE_NAME, SCHEMA_NAME, CHECK_TYPE, STATUS, DETAILS, RUN_TIMESTAMP)
                      VALUES (?, ?, 'NULL_CHECK', ?, ?, CURRENT_TIMESTAMP)`,
            binds: [tableName, schemaName, status, `STG_NULL=${n1}, INT_NULL=${n2}, HIST_NULL=${n3}`]
        }).execute();

        // ----------------------
        // STRUCTURE CHECK
        // ----------------------
        var sql_cols_stg = `SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA='${stgTable.split('.')[1]}' 
                              AND TABLE_NAME='${stgTable.split('.')[2]}'`;

        var sql_cols_int = `SELECT COLUMN_NAME 
                            FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA='${intTable.split('.')[1]}' 
                              AND TABLE_NAME='${intTable.split('.')[2]}'`;

        var sql_cols_hist = `SELECT COLUMN_NAME 
                             FROM INFORMATION_SCHEMA.COLUMNS 
                             WHERE TABLE_SCHEMA='${histTable.split('.')[1]}' 
                               AND TABLE_NAME='${histTable.split('.')[2]}' 
                               AND COLUMN_NAME NOT IN ('INSERT_TIMESTAMP','VALID_FROM','VALID_TO','DML_TYPE','CURRENT_FLAG')`;

        var cols_stg = snowflake.createStatement({sqlText: sql_cols_stg}).execute();
        var cols_int = snowflake.createStatement({sqlText: sql_cols_int}).execute();
        var cols_hist = snowflake.createStatement({sqlText: sql_cols_hist}).execute();

        var list_stg=[], list_int=[], list_hist=[];
        while (cols_stg.next()) { list_stg.push(cols_stg.getColumnValue(1)); }
        while (cols_int.next()) { list_int.push(cols_int.getColumnValue(1)); }
        while (cols_hist.next()) { list_hist.push(cols_hist.getColumnValue(1)); }

        var struct_ok = (list_stg.sort().join() == list_int.sort().join() && list_int.sort().join() == list_hist.sort().join());
        var status = struct_ok ? 'PASS' : 'FAIL';

        snowflake.createStatement({
            sqlText: `INSERT INTO DATABASE_NAME.SCHEMA_NAME.DQ_RESULTS_TEST 
                      (TABLE_NAME, SCHEMA_NAME, CHECK_TYPE, STATUS, DETAILS, RUN_TIMESTAMP)
                      VALUES (?, ?, 'STRUCTURE_CHECK', ?, ?, CURRENT_TIMESTAMP)`,
            binds: [tableName, schemaName, status, `STG_COLS=${list_stg.length}, INT_COLS=${list_int.length}, HIST_COLS=${list_hist.length}`]
        }).execute();

        // ----------------------
        // DATA COMPARISON (INT vs HIST)
        // ----------------------
        var sql_diff = `
            SELECT COUNT(*) FROM (
              (SELECT * EXCLUDE (INSERT_TIMESTAMP, VALID_FROM, VALID_TO, DML_TYPE, CURRENT_FLAG)
               FROM ${histTable} WHERE CURRENT_FLAG='Y')
              MINUS
              (SELECT * FROM ${intTable})
              UNION
              (SELECT * FROM ${intTable})
              MINUS
              (SELECT * EXCLUDE (INSERT_TIMESTAMP, VALID_FROM, VALID_TO, DML_TYPE, CURRENT_FLAG)
               FROM ${histTable} WHERE CURRENT_FLAG='Y')
            )
        `;

        var rs_diff = snowflake.createStatement({sqlText: sql_diff}).execute(); rs_diff.next();
        var diff_cnt = rs_diff.getColumnValue(1);

        var status = (diff_cnt == 0) ? 'PASS' : 'FAIL';

        snowflake.createStatement({
            sqlText: `INSERT INTO DATABASE_NAME.SCHEMA_NAME.DQ_RESULTS_TEST 
                      (TABLE_NAME, SCHEMA_NAME, CHECK_TYPE, STATUS, DETAILS, RUN_TIMESTAMP)
                      VALUES (?, ?, 'DATA_COMPARISON', ?, ?, CURRENT_TIMESTAMP)`,
            binds: [tableName, schemaName, status, `DIFF_COUNT=${diff_cnt}`]
        }).execute();
    }

    return 'DQ Checks Completed!';
} catch (err) {
    return 'Failed: ' + err;
}
$$;
