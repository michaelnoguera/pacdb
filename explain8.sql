-- 8) Shuffle keeping all, use LATERAL to return two rows per shuffle sample
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain8.json';
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
        l.sample_id * 2 + part AS sample_id,
        array_sort(row_ids) AS row_ids
        FROM shuffled_rows_to_split l,
        LATERAL (
            SELECT 
                part,
                CASE part
                    WHEN 0 THEN l.shuffled_row_ids[:array_length(l.shuffled_row_ids) / 2]
                    ELSE l.shuffled_row_ids[array_length(l.shuffled_row_ids) / 2:]
                END AS row_ids
            FROM (VALUES (0), (1)) AS v(part)
        ) AS split
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