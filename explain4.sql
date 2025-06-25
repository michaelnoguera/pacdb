-- 4) TPCH customers, strict 50% sample, CTE materialized, unnested at end, case statement
-- 5.23s https://db.cs.uni-tuebingen.de/explain/plan/afdged77cc56a4a7
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain4.json';
PRAGMA profiling_mode = 'detailed';

WITH selected_rows AS MATERIALIZED (
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
    CASE 
        WHEN customer.rowid IN (SELECT unnest(sr2.row_ids) FROM selected_rows sr2 WHERE sr2.sample_id = sr.sample_id) THEN TRUE 
        ELSE FALSE 
    END AS random_binary
FROM
    customer
CROSS JOIN selected_rows AS sr
ORDER BY
    sr.sample_id,
    customer.rowid;