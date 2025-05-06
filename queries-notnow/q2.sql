--var:SAMPLES = 1024
--var:INDEX_COLS = ['s_name', 'p_partkey', 'n_name', 's_acctbal']
--var:OUTPUT_COLS = ['min(ps_supplycost)']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(1024 // 2)
), random_values AS MATERIALIZED (
    SELECT 
        sample_numbers.sample_id,
        partsupp.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN partsupp ON TRUE  -- Cross join to duplicate rows for each sample
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
--end SAMPLE_STEP--


--begin PREPARE_STEP--
DEALLOCATE PREPARE run_query;

PREPARE run_query AS 
SELECT
    s_name,
    p_partkey,
    n_name,
    s_acctbal,
    min(ps_supplycost)
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
            (SELECT * FROM partsupp
              JOIN random_samples AS rs ON rs.row_id = partsupp.rowid
              AND rs.random_binary = TRUE
              AND rs.sample_id = $sample) as partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
GROUP BY s_acctbal,
    s_name,
    n_name,
    p_partkey;
--end PREPARE_STEP--

EXECUTE run_query(sample := 0);