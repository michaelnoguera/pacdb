-- Modified TPC-H q1 to run on 50% subsamples


--var:SAMPLES = 1024
--var:INDEX_COLS = ['l_returnflag', 'l_linestatus']
--var:OUTPUT_COLS = ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']


--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
WITH shuffled_rows_to_split AS MATERIALIZED (
    SELECT 
        sample_id,
        array_agg(customer.rowid ORDER BY RANDOM()) AS shuffled_row_ids
    FROM 
        customer,
        range(512) AS t(sample_id)
    GROUP BY sample_id
),
splits AS (
    SELECT
        sample_id * 2 AS sample_id_a,
        sample_id * 2 + 1 AS sample_id_b,
        shuffled_row_ids[:len(shuffled_row_ids) // 2] AS row_ids_a,
        shuffled_row_ids[len(shuffled_row_ids) // 2:] AS row_ids_b
    FROM shuffled_rows_to_split
),
selected_rows AS (
    SELECT
        sample_id_a AS sample_id,
        array_sort(row_ids_a) AS row_ids
    FROM splits
    UNION ALL
    SELECT
        sample_id_b AS sample_id,
        array_sort(row_ids_b) AS row_ids
    FROM splits
    ORDER BY sample_id
)
SELECT
    sr.sample_id,
    customer.rowid AS row_id,
    CASE 
        WHEN customer.rowid IN (
            SELECT unnest(sr2.row_ids)
            FROM selected_rows sr2
            WHERE sr2.sample_id = sr.sample_id
        ) THEN TRUE 
        ELSE FALSE 
    END AS random_binary
FROM
    customer,
    selected_rows AS sr
ORDER BY
    sr.sample_id,
    customer.rowid;
--end SAMPLE_STEP--


--begin PREPARE_STEP--
DEALLOCATE PREPARE run_query;

PREPARE run_query AS 
SELECT
    l_returnflag,
    l_linestatus,
    2*sum(l_quantity) AS sum_qty,
    2*sum(l_extendedprice) AS sum_base_price,
    2*sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    2*sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    2*count(*) AS count_order
FROM
    lineitem
JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
JOIN customer ON orders.o_custkey = customer.c_custkey
JOIN random_samples AS rs
    ON rs.row_id = customer.rowid
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);
