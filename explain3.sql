-- 3) TPCH customers, strict 50% sample, CTE materialized, packed in arrays, array_agg
-- 30.81s https://db.cs.uni-tuebingen.de/explain/plan/fed272g1d5f2gfgb
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain3.json';
PRAGMA profiling_mode = 'detailed';

WITH selected_rows AS MATERIALIZED (
    SELECT 
        sample_id,
        array_sort(array_slice(array_agg(customer.rowid ORDER BY RANDOM()), 1, (SELECT COUNT(*) / 2 FROM customer))) AS row_ids
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
    sample_id,
    array_agg(
        array_contains(row_ids, customer.rowid)::BOOLEAN
    ) AS mask
FROM
    selected_rows AS sr
CROSS JOIN
    customer
GROUP BY
    sample_id
ORDER BY
    sample_id;