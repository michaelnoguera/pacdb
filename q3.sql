--Shipping Priority Query (Q3) This query retrieves the 10 unshipped orders with the highest value.

DROP TABLE IF EXISTS random_samples;
DEALLOCATE PREPARE run_query;

--PRAGMA enable_profiling = 'json';
--PRAGMA profiling_mode = 'detailed';
--PRAGMA profiling_output = '/Users/michael/projects/dpdb/pacdb/randomtable.json';

.mode csv
.timer on

.print "Creating sample table"
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

.print "Defining query as prepared statement"
PREPARE run_query AS 
SELECT
    --$sample,
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer, --TABLESAMPLE bernoulli(50%),
    orders,
    lineitem
JOIN random_samples AS rs
    ON rs.row_id = customer.rowid
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample    
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;

EXECUTE run_query(sample := 1);
EXECUTE run_query(sample := 2);
.exit