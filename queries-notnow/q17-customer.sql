--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['avg_yearly']

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

PREPARE run_query AS 
WITH lineitem_sampled AS (
    SELECT * FROM lineitem
    JOIN orders ON l_orderkey = o_orderkey
    JOIN customer c ON o_custkey = c.c_custkey
    JOIN random_samples rs ON c.rowid = rs.row_id 
        AND rs.sample_id = $sample 
        AND rs.random_binary = TRUE
)
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem_sampled,
    part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            lineitem_sampled
        WHERE
            l_partkey = p_partkey);
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);