--var:SAMPLES = 1024
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['l_orderkey','o_orderdate','o_shippriority', 'revenue']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(1024)
), random_values AS MATERIALIZED (
    SELECT 
        sample_numbers.sample_id,
        customer.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN customer ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id,
    random_binary
FROM random_values
--end SAMPLE_STEP--

--begin PREPARE_STEP--
DEALLOCATE PREPARE run_query;

PREPARE run_query AS 
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    (SELECT * FROM customer
        JOIN random_samples AS rs ON rs.row_id = customer.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample) AS customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate;
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);