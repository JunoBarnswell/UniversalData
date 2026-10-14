-- Read-only checks for a disposable MySQL 8+ test database.
-- Enter the actual non-secret run_id that UniversalData inserted.
SET @ud_ssis_run_id = 'REPLACE_WITH_ACTUAL_RUN_ID';

SELECT 'vip' AS branch, COUNT(*) AS row_count
FROM ud_ssis_vip WHERE run_id = @ud_ssis_run_id
UNION ALL SELECT 'refund', COUNT(*) FROM ud_ssis_refund WHERE run_id = @ud_ssis_run_id
UNION ALL SELECT 'paid_other', COUNT(*) FROM ud_ssis_paid_other WHERE run_id = @ud_ssis_run_id
UNION ALL SELECT 'other', COUNT(*) FROM ud_ssis_other WHERE run_id = @ud_ssis_run_id;

SELECT SUM(n) AS main_table_total FROM (
    SELECT COUNT(*) AS n FROM ud_ssis_vip WHERE run_id = @ud_ssis_run_id
    UNION ALL SELECT COUNT(*) FROM ud_ssis_refund WHERE run_id = @ud_ssis_run_id
    UNION ALL SELECT COUNT(*) FROM ud_ssis_paid_other WHERE run_id = @ud_ssis_run_id
    UNION ALL SELECT COUNT(*) FROM ud_ssis_other WHERE run_id = @ud_ssis_run_id
) AS branch_counts;

SELECT source_row_id, COUNT(*) AS duplicates FROM (
    SELECT source_row_id FROM ud_ssis_vip WHERE run_id = @ud_ssis_run_id
    UNION ALL SELECT source_row_id FROM ud_ssis_refund WHERE run_id = @ud_ssis_run_id
    UNION ALL SELECT source_row_id FROM ud_ssis_paid_other WHERE run_id = @ud_ssis_run_id
    UNION ALL SELECT source_row_id FROM ud_ssis_other WHERE run_id = @ud_ssis_run_id
) AS branches
GROUP BY source_row_id HAVING COUNT(*) <> 1;

SELECT region, input_rows, amount_sum FROM ud_ssis_region_summary
WHERE run_id = @ud_ssis_run_id ORDER BY region;
SELECT product_code, input_rows, amount_sum FROM ud_ssis_product_summary
WHERE run_id = @ud_ssis_run_id ORDER BY product_code;
SELECT COUNT(*) AS error_count FROM ud_ssis_errors WHERE run_id = @ud_ssis_run_id;

-- Check actual main-table data against individual and combined regional aggregates:
SELECT region, COUNT(*) AS rows_written, SUM(amount) AS amount_sum FROM (
  SELECT region, amount FROM ud_ssis_vip WHERE run_id = @ud_ssis_run_id
  UNION ALL SELECT region, amount FROM ud_ssis_refund WHERE run_id = @ud_ssis_run_id
  UNION ALL SELECT region, amount FROM ud_ssis_paid_other WHERE run_id = @ud_ssis_run_id
  UNION ALL SELECT region, amount FROM ud_ssis_other WHERE run_id = @ud_ssis_run_id
) AS data_by_region GROUP BY region ORDER BY region;

-- RowId gap detection must additionally join against ssis_50000_clean.csv
-- or ssis_50000_dirty.csv RowIds and error Rows. Counts alone are insufficient.
