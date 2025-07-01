--var:SAMPLES = 1024
--var:INDEX_COLS = ['c_name', 'c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice']
--var:OUTPUT_COLS = ['sum(l_quantity)']


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
WITH lineitem_sample as (
    SELECT lineitem.*
    FROM lineitem,
        random_samples AS rs
    WHERE
        rs.row_id = lineitem.rowid
        AND rs.random_binary = TRUE
        AND rs.sample_id = $sample
)
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    customer,
    orders,
    lineitem_sample
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            lineitem_sample
        GROUP BY
            l_orderkey
        HAVING
            sum(l_quantity) > 300)
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate;
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);