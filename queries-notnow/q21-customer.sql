--var:SAMPLES = 1024
--var:INDEX_COLS = ['s_name']
--var:OUTPUT_COLS = ['numwait']

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
ORDER BY sample_id, row_id;
--end SAMPLE_STEP--

--begin PREPARE_STEP--
DEALLOCATE PREPARE run_query;

PREPARE run_query AS 
WITH lineitem_sample as (
    SELECT lineitem.*
    FROM lineitem,
        orders,
        customer,
        random_samples AS rs
    WHERE
        rs.row_id = customer.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample
        AND o_custkey = c_custkey
        AND o_orderkey = l_orderkey
)
SELECT
    s_name,
    count(*) AS numwait
FROM
    supplier,
    lineitem_sample l1,
    orders,
    nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem_sample l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem_sample l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate)
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name;
--end PREPARE_STEP--

EXECUTE run_query(sample := 0);