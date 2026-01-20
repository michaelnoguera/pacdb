--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['avg_yearly']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;
DROP TABLE IF EXISTS lineitem_enhanced;

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
    row_id
FROM random_values
WHERE random_binary = TRUE
ORDER BY sample_id, row_id;

CREATE TEMP TABLE lineitem_enhanced AS
SELECT l.l_orderkey, 
    l.l_linenumber,
    l.l_partkey,
    l.l_quantity,
    l.l_extendedprice,
    c.rowid as c_rowid,
FROM lineitem l
JOIN orders o ON o.o_orderkey = l.l_orderkey
JOIN customer c ON c.c_custkey = o.o_custkey
ORDER BY l.l_orderkey, l.l_linenumber;
--end SAMPLE_STEP--


--begin PREPARE_STEP--
DEALLOCATE PREPARE run_query;

PREPARE run_query AS 
WITH lineitem_sampled AS (
    SELECT * FROM lineitem_enhanced le
    JOIN random_samples rs ON le.c_rowid = rs.row_id 
        AND rs.sample_id = $sample 
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