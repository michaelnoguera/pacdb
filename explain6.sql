-- 6) Shuffle keeping all, partition, and union all
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain6.json';
PRAGMA profiling_mode = 'detailed';

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