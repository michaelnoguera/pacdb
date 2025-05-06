--var:SAMPLES = 1024
--var:INDEX_COLS = ['rank']
--var:OUTPUT_COLS = ['revenue']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(1024 // 2)
), random_values AS MATERIALIZED (
    SELECT 
        sample_numbers.sample_id,
        orders.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN orders ON TRUE  -- Cross join to duplicate rows for each sample
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
    ROW_NUMBER() OVER (ORDER BY SUM(l_extendedprice * (1 - l_discount)) DESC, o_orderdate) AS rank,
    --l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    --o_orderdate,
    --o_shippriority
FROM
    customer,
    (SELECT * FROM orders
              JOIN random_samples AS rs ON rs.row_id = orders.rowid
              AND rs.random_binary = TRUE
              AND rs.sample_id = $sample) as orders,
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
    o_orderdate
LIMIT 10;
--end PREPARE_STEP--

EXECUTE run_query(sample := 0);