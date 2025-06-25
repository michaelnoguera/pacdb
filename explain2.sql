-- 2) TPCH customers, strict 50% sample, CTE materialized, packed in arrays
-- 3.47s https://db.cs.uni-tuebingen.de/explain/plan/28fcdc04eh55ahe9
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'explain2.json';
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
    sample_id,
    array(
        SELECT 
            CASE 
                WHEN customer.rowid IN (
                    SELECT unnest(row_ids)
                    FROM selected_rows
                    WHERE sample_id = sr.sample_id
                ) THEN TRUE 
                ELSE FALSE 
            END
        FROM 
            customer
    ) AS mask
FROM
    selected_rows AS sr
GROUP BY
    sample_id
ORDER BY
    sample_id;