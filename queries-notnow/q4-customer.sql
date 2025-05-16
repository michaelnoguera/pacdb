--var:SAMPLES = 1024
--var:INDEX_COLS = ['o_orderpriority']
--var:OUTPUT_COLS = ['order_count']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(1024 // 2)
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
    o_orderpriority,
    count(*) AS order_count
FROM
    orders,
    (SELECT * FROM customer
        JOIN random_samples AS rs ON rs.row_id = customer.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample) AS customer
WHERE
    c_custkey = o_custkey
    AND o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
--end PREPARE_STEP--

EXECUTE run_query(sample := 0);