-- 1) TPCH customers, strict 50% sample, CTE not materialized
-- 33.38s https://db.cs.uni-tuebingen.de/explain/plan/38bed8612a3684h6
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain1.json';
PRAGMA profiling_mode = 'detailed';

WITH selected_rows AS (
    SELECT 
        sample_id,
        array_sort(array_slice(
            array_agg(customer.rowid ORDER BY RANDOM()),
            1,
            (SELECT COUNT(*) / 2 FROM customer)
        )) AS row_ids
    FROM 
        range(1024) AS t(sample_id)
    CROSS JOIN
        customer
    GROUP BY 
        sample_id
    ORDER BY 
        sample_id
)
SELECT
    sr.sample_id,
    customer.rowid AS row_id,
    array_contains(sr.row_ids, customer.rowid)::BOOLEAN AS random_binary
FROM
    customer
CROSS JOIN selected_rows AS sr
ORDER BY
    sr.sample_id,
    customer.rowid;