-- 5) Shuffle, take half, list subtract from customer.rowid to get complement
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain5.json';
PRAGMA profiling_mode = 'detailed';

WITH full_index_list AS MATERIALIZED (
    SELECT array_agg(rowid ORDER BY rowid) AS all_ids
    FROM customer
),
first_half_selected_rows AS MATERIALIZED (
    SELECT 
        sample_id,
        array_sort(array_slice(
            array_agg(customer.rowid ORDER BY RANDOM()),
            1,
            (SELECT COUNT(*) // 2 FROM customer)
        )) AS row_ids
    FROM 
        customer,
        range(1024 // 2) AS t(sample_id)
    GROUP BY sample_id
    ORDER BY sample_id
),
selected_rows AS (
    SELECT
        sample_id * 2 AS sample_id,
        row_ids
    FROM first_half_selected_rows
    UNION ALL
    SELECT
        sample_id * 2 + 1 AS sample_id,
        list_filter(all_ids, x -> NOT list_contains(row_ids, x)) AS row_ids
    FROM first_half_selected_rows, full_index_list
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