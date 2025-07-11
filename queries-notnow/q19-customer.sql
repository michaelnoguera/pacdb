--var:SAMPLES = 128
--var:INDEX_COLS = []
--var:OUTPUT_COLS = ['revenue']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
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
SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part,
    orders,
    customer,
    random_samples AS rs
WHERE rs.row_id = customer.rowid
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
    AND o_custkey = c_custkey
    AND o_orderkey = l_orderkey
    AND ((p_partkey = l_partkey
    AND p_brand = 'Brand#12'
    AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND l_quantity >= 1
    AND l_quantity <= 1 + 10
    AND p_size BETWEEN 1 AND 5
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'));
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);