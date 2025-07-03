--PRAGMA enable_profiling = 'json';
--PRAGMA profiling_output = 'explain-q5-2.json';
--PRAGMA profiling_mode = 'detailed';

--var:SAMPLES = 1024
--var:INDEX_COLS = ['n_name']
--var:OUTPUT_COLS = ['revenue']

--begin SAMPLE_STEP--
DROP TABLE IF EXISTS random_samples;

--.timer on
CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range(1024)
), random_values AS MATERIALIZED (
    SELECT
        sample_numbers.sample_id,
        customer.rowid AS row_id,
        (RANDOM() > 0.5) AS random_binary
    FROM sample_numbers
    JOIN customer ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id
FROM random_values
WHERE random_binary = TRUE
ORDER BY sample_id, row_id;
--end SAMPLE_STEP--

--.timer off
DROP TABLE IF EXISTS prejoined;

--.timer on
CREATE TABLE prejoined AS
SELECT
    customer.rowid as customer_row_id,
    n_name,
    l_extendedprice * (1 - l_discount) AS customer_nation_revenue
FROM 
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
ORDER BY
    customer_row_id;

--.timer off
DEALLOCATE PREPARE run_query;

--.timer on
PREPARE run_query AS
SELECT
    n_name,
    sum(customer_nation_revenue) AS revenue
FROM
    random_samples AS rs
JOIN
    prejoined ON rs.row_id = prejoined.customer_row_id
WHERE
    rs.sample_id = $sample
GROUP BY
    n_name
ORDER BY
    revenue DESC;
--end PREPARE_STEP--


EXECUTE run_query(sample := 0);
EXECUTE run_query(sample := 1);
EXECUTE run_query(sample := 2);
EXECUTE run_query(sample := 3);
EXECUTE run_query(sample := 4);
EXECUTE run_query(sample := 5);
EXECUTE run_query(sample := 6);
EXECUTE run_query(sample := 7);
EXECUTE run_query(sample := 8);
EXECUTE run_query(sample := 9);
EXECUTE run_query(sample := 10);
EXECUTE run_query(sample := 11);
EXECUTE run_query(sample := 12);
EXECUTE run_query(sample := 13);
EXECUTE run_query(sample := 14);
EXECUTE run_query(sample := 15);
EXECUTE run_query(sample := 16);
EXECUTE run_query(sample := 17);
EXECUTE run_query(sample := 18);
EXECUTE run_query(sample := 19);
EXECUTE run_query(sample := 20);
EXECUTE run_query(sample := 21);
EXECUTE run_query(sample := 22);
EXECUTE run_query(sample := 23);
EXECUTE run_query(sample := 24);
EXECUTE run_query(sample := 25);
EXECUTE run_query(sample := 26);
EXECUTE run_query(sample := 27);
EXECUTE run_query(sample := 28);
EXECUTE run_query(sample := 29);
EXECUTE run_query(sample := 30);
EXECUTE run_query(sample := 31);
EXECUTE run_query(sample := 32);
EXECUTE run_query(sample := 33);
EXECUTE run_query(sample := 34);
EXECUTE run_query(sample := 35);
EXECUTE run_query(sample := 36);
EXECUTE run_query(sample := 37);
EXECUTE run_query(sample := 38);
EXECUTE run_query(sample := 39);
EXECUTE run_query(sample := 40);
EXECUTE run_query(sample := 41);
EXECUTE run_query(sample := 42);
EXECUTE run_query(sample := 43);
EXECUTE run_query(sample := 44);
EXECUTE run_query(sample := 45);
EXECUTE run_query(sample := 46);
EXECUTE run_query(sample := 47);
EXECUTE run_query(sample := 48);
EXECUTE run_query(sample := 49);
EXECUTE run_query(sample := 50);
EXECUTE run_query(sample := 51);
EXECUTE run_query(sample := 52);
EXECUTE run_query(sample := 53);
EXECUTE run_query(sample := 54);
EXECUTE run_query(sample := 55);
EXECUTE run_query(sample := 56);
EXECUTE run_query(sample := 57);
EXECUTE run_query(sample := 58);
EXECUTE run_query(sample := 59);
EXECUTE run_query(sample := 60);
EXECUTE run_query(sample := 61);
EXECUTE run_query(sample := 62);
EXECUTE run_query(sample := 63);
EXECUTE run_query(sample := 64);
EXECUTE run_query(sample := 65);
EXECUTE run_query(sample := 66);
EXECUTE run_query(sample := 67);
EXECUTE run_query(sample := 68);
EXECUTE run_query(sample := 69);
EXECUTE run_query(sample := 70);
EXECUTE run_query(sample := 71);
EXECUTE run_query(sample := 72);
EXECUTE run_query(sample := 73);
EXECUTE run_query(sample := 74);
EXECUTE run_query(sample := 75);
EXECUTE run_query(sample := 76);
EXECUTE run_query(sample := 77);
EXECUTE run_query(sample := 78);
EXECUTE run_query(sample := 79);
EXECUTE run_query(sample := 80);
EXECUTE run_query(sample := 81);
EXECUTE run_query(sample := 82);
EXECUTE run_query(sample := 83);
EXECUTE run_query(sample := 84);
EXECUTE run_query(sample := 85);
EXECUTE run_query(sample := 86);
EXECUTE run_query(sample := 87);
EXECUTE run_query(sample := 88);
EXECUTE run_query(sample := 89);
EXECUTE run_query(sample := 90);
EXECUTE run_query(sample := 91);
EXECUTE run_query(sample := 92);
EXECUTE run_query(sample := 93);
EXECUTE run_query(sample := 94);
EXECUTE run_query(sample := 95);
EXECUTE run_query(sample := 96);
EXECUTE run_query(sample := 97);
EXECUTE run_query(sample := 98);
EXECUTE run_query(sample := 99);