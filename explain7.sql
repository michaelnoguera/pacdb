-- 7) Shuffle keeping all, keep everything as lists and unnest at end
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain7.json';
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
selected_rows AS (
    SELECT
        sample_id * 2 + part AS sample_id,
        array_sort(row_ids) AS row_ids
    FROM shuffled_rows_to_split,
    UNNEST(
        list_zip(
            [0, 1],
            [
                shuffled_row_ids[:len(shuffled_row_ids) // 2],
                shuffled_row_ids[len(shuffled_row_ids) // 2:]
            ]
        )
    ) AS t(zipped),
    LATERAL (
        SELECT zipped[1] AS part, zipped[2] AS row_ids
    )
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