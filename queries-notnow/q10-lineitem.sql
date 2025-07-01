--var:SAMPLES = 1024
--var:INDEX_COLS = ['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment']
--var:OUTPUT_COLS = ['revenue']


--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(1024)
), random_values AS MATERIALIZED (
    SELECT 
        sample_numbers.sample_id,
        lineitem.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN lineitem ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id,
    random_binary
FROM random_values
ORDER BY sample_id, row_id;
--end SAMPLE_STEP--

--begin PREPARE_STEP--
DEALLOCATE PREPARE run_query;

PREPARE run_query AS
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
JOIN random_samples AS rs
    ON rs.row_id = lineitem.rowid
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= CAST('1993-10-01' AS date)
    AND o_orderdate < CAST('1994-01-01' AS date)
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment;
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);