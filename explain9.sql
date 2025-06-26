-- 9) Shuffle keeping first half, union all after converting to booleans
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain9.json';
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
first_half_selected_rows AS (
    SELECT
        sample_id,
        shuffled_row_ids[:len(shuffled_row_ids) // 2] as row_ids
    FROM shuffled_rows_to_split
),
booleans AS (
    SELECT
        sr.sample_id * 2 AS sample_id,
        customer.rowid AS row_id,
        CASE
            WHEN customer.rowid IN (
                SELECT unnest(sr2.row_ids)
                FROM first_half_selected_rows sr2
                WHERE sr.sample_id = sr2.sample_id
            ) THEN TRUE
            ELSE FALSE
        END AS random_binary
    FROM
        customer,
        shuffled_rows_to_split as sr
),
inverse_booleans AS (
    SELECT
        sample_id + 1 AS sample_id,
        row_id,
        NOT random_binary AS random_binary
    FROM booleans
)
SELECT * FROM booleans
UNION ALL
SELECT * FROM inverse_booleans
ORDER BY sample_id, row_id;