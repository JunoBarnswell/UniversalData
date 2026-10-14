-- UniversalData SSIS strict-parity, 50,000-row *isolated test database*.
-- Run only against a designated disposable MySQL 8+ test database.
-- All input business string data uses utf8mb4, binary collation to avoid
-- allowing MySQL collation to change Native SSIS Conditional Split decisions.
-- DO NOT write any secrets in this SQL or any Package JSON.

CREATE TABLE IF NOT EXISTS ud_ssis_vip (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_row_id BIGINT UNSIGNED NOT NULL,
    order_no VARCHAR(24) NOT NULL,
    customer_name VARCHAR(128) NOT NULL,
    original_name_length INT UNSIGNED NOT NULL,
    first_character VARCHAR(16) NOT NULL,
    name_token_count INT NOT NULL,
    middle_category VARCHAR(80) NOT NULL,
    status_label VARCHAR(32) NOT NULL,
    is_negative_amount TINYINT(1) NOT NULL,
    tax_amount DECIMAL(38,2) NULL,
    order_month TINYINT UNSIGNED NOT NULL,
    status VARCHAR(16) NULL,
    region VARCHAR(8) NOT NULL,
    product_code VARCHAR(8) NOT NULL,
    amount DECIMAL(38,2) NULL,
    category_path VARCHAR(128) NOT NULL,
    created_at DATETIME NOT NULL,
    remark TEXT NULL,
    PRIMARY KEY(run_id, source_row_id),
    INDEX idx_vip_region(run_id, region),
    INDEX idx_vip_product(run_id, product_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS ud_ssis_refund LIKE ud_ssis_vip;
CREATE TABLE IF NOT EXISTS ud_ssis_paid_other LIKE ud_ssis_vip;
CREATE TABLE IF NOT EXISTS ud_ssis_other LIKE ud_ssis_vip;

CREATE TABLE IF NOT EXISTS ud_ssis_region_summary (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    region VARCHAR(8) NOT NULL,
    input_rows BIGINT UNSIGNED NOT NULL,
    amount_sum DECIMAL(38,2) NULL,
    PRIMARY KEY(run_id, region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS ud_ssis_product_summary (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    product_code VARCHAR(8) NOT NULL,
    input_rows BIGINT UNSIGNED NOT NULL,
    amount_sum DECIMAL(38,2) NULL,
    PRIMARY KEY(run_id, product_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS ud_ssis_errors (
    run_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_row_id BIGINT UNSIGNED NOT NULL,
    stage VARCHAR(48) NOT NULL,
    error_kind VARCHAR(32) NOT NULL,
    error_code VARCHAR(64) NOT NULL,
    error_column VARCHAR(128) NOT NULL,
    source_position VARCHAR(128) NULL,
    raw_amount VARCHAR(128) NULL,
    raw_record LONGTEXT NULL,
    PRIMARY KEY(run_id, source_row_id, stage, error_column)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
