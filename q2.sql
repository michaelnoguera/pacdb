DROP TABLE IF EXISTS random_samples;
DEALLOCATE PREPARE run_query;

.mode csv
.timer on

.print "Creating sample table"
CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(1024 // 2)
), random_values AS MATERIALIZED (
    SELECT 
        sample_numbers.sample_id,
        supplier.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN supplier ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id,
    random_binary
FROM random_values
UNION ALL
SELECT -- select the complementary samples too
    (1024 // 2) + sample_id,
    row_id,
    NOT random_binary  -- Inverse the random_binary to get the complementary sample
FROM random_values
ORDER BY sample_id, row_id;

.print "Defining query as prepared statement"
PREPARE run_query AS 
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        JOIN random_samples AS rs ON rs.row_id = supplier.rowid
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
            AND rs.random_binary = TRUE
            AND rs.sample_id = $sample
        )
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;

.print "Example samples"
EXECUTE run_query(sample := 0);
EXECUTE run_query(sample := 1);
--EXECUTE run_query(sample := 2);
--EXECUTE run_query(sample := 3);
.exit