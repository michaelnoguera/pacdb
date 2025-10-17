--var:SAMPLES = 128
--var:INDEX_COLS = ['s_name']
--var:OUTPUT_COLS = ['numwait']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TEMP TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(128)
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

CREATE TEMP TABLE lineitem_sample AS
SELECT rs.sample_id as sample_id, 
    lineitem.l_suppkey,
    lineitem.l_orderkey,
    lineitem.l_receiptdate,
    lineitem.l_commitdate
FROM lineitem,
    orders,
    customer,
    random_samples AS rs
WHERE
    rs.row_id = customer.rowid
    AND rs.random_binary = TRUE
    AND o_custkey = c_custkey
    AND o_orderkey = l_orderkey
ORDER BY rs.sample_id, lineitem.rowid;


PREPARE run_query AS
SELECT
    s.s_name,
    count(*) AS numwait
FROM
    (
        SELECT
            l1.l_suppkey,
            l1.l_orderkey
        FROM
            lineitem_sample l1
            JOIN orders o ON o.o_orderkey = l1.l_orderkey
            JOIN supplier s ON s.s_suppkey = l1.l_suppkey
            JOIN nation n ON s.s_nationkey = n.n_nationkey
        WHERE
            l1.sample_id = $sample
            AND o.o_orderstatus = 'F'
            AND l1.l_receiptdate > l1.l_commitdate
            AND n.n_name = 'SAUDI ARABIA'
            AND EXISTS (
                SELECT 1
                FROM lineitem_sample l2
                WHERE
                    l2.sample_id = $sample
                    AND l2.l_orderkey = l1.l_orderkey
                    AND l2.l_suppkey <> l1.l_suppkey
            )
            AND NOT EXISTS (
                SELECT 1
                FROM lineitem_sample l3
                WHERE
                    l3.sample_id = $sample
                    AND l3.l_orderkey = l1.l_orderkey
                    AND l3.l_suppkey <> l1.l_suppkey
                    AND l3.l_receiptdate > l3.l_commitdate
            )
    ) xx
    JOIN supplier s ON s.s_suppkey = xx.l_suppkey
GROUP BY
    s.s_name
ORDER BY
    numwait DESC,
    s.s_name;
--end PREPARE_STEP--

EXECUTE run_query(sample := 0);