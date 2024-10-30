CREATE TABLE customer_info (
    id_client INT PRIMARY KEY,
    total_amount NUMERIC,
    gender CHAR(1),
    age NUMERIC,
    count_city INT,
    response_communication INT,
    communication_3month INT,
    tenure INT
);

CREATE TABLE transaction_info (
    date_new DATE,
    id_check INT,
    id_client INT,
    count_products NUMERIC,
    sum_payment NUMERIC,
    -- PRIMARY KEY (date_new, id_check, id_client),
    FOREIGN KEY (id_client) REFERENCES customer_info(id_client)
);
drop table transaction_info;

COPY customer_info (id_client, total_amount, gender, age, count_city, response_communication, communication_3month, tenure)
FROM 'C:/Program Files/PostgreSQL/customer_info.xlsx - QUERY_FOR_ABT_CUSTOMERINFO_0002 (1).csv'
DELIMITER ','
CSV HEADER;

COPY transaction_info (date_new, id_check, id_client, count_products, sum_payment)
FROM 'C:/Program Files/PostgreSQL/transactions_info.xlsx - TRANSACTIONS (1) (1).csv'
DELIMITER ','
CSV HEADER;

SHOW data_directory;

SELECT * FROM transaction_info;
SELECT * FROM customer_info;

WITH monthly_transactions AS (
    SELECT 
        id_client,
        DATE_TRUNC('month', date_new) AS month,
        COUNT(id_check) AS transactions_count,
        SUM(sum_payment) AS total_amount
    FROM 
        transaction_info
    WHERE 
        date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY 
        id_client, DATE_TRUNC('month', date_new)
),
clients_with_continuous_history AS (
    SELECT 
        id_client
    FROM 
        monthly_transactions
    GROUP BY 
        id_client
    HAVING 
        COUNT(DISTINCT month) = 12
)
SELECT 
    c.id_client,
    AVG(mt.total_amount) AS average_receipt,  -- Средний чек за период
    SUM(mt.total_amount) / 12 AS average_monthly_purchases,  -- Средняя сумма покупок за месяц
    SUM(mt.transactions_count) AS total_transactions  -- Общее количество операций за период
FROM 
    clients_with_continuous_history c
JOIN 
    monthly_transactions mt ON c.id_client = mt.id_client
GROUP BY 
    c.id_client;


WITH monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', date_new) AS month,
        COUNT(id_check) AS total_transactions,
        SUM(sum_payment) AS total_amount,
        AVG(sum_payment) AS average_receipt,
        COUNT(DISTINCT id_client) AS unique_customers
    FROM 
        transaction_info
    WHERE 
        date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY 
        DATE_TRUNC('month', date_new)
),
yearly_stats AS (
    SELECT 
        SUM(total_transactions) AS yearly_total_transactions,
        SUM(total_amount) AS yearly_total_amount
    FROM 
        monthly_stats
)
SELECT 
    ms.month,
    ms.average_receipt,
    ms.total_transactions / COUNT(ms.month) OVER() AS average_operations_per_month,
    ms.unique_customers,
    (ms.total_transactions::FLOAT / ys.yearly_total_transactions) * 100 AS transaction_share,
    (ms.total_amount::FLOAT / ys.yearly_total_amount) * 100 AS amount_share
FROM 
    monthly_stats ms, yearly_stats ys;



SELECT 
    DATE_TRUNC('month', t.date_new) AS month,
    c.gender,
    COUNT(t.id_check) AS transaction_count,
    SUM(t.sum_payment) AS total_amount,
    (SUM(t.sum_payment) / (SELECT SUM(sum_payment) FROM transaction_info WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01')) * 100 AS cost_share
FROM 
    transaction_info t
JOIN 
    customer_info c ON t.id_client = c.id_client
WHERE 
    t.date_new BETWEEN '2015-06-01' AND '2016-06-01'
GROUP BY 
    month, c.gender
ORDER BY 
    month, c.gender;



WITH age_groups AS (
    SELECT 
        id_client,
        CASE 
            WHEN age BETWEEN 0 AND 9 THEN '0-9'
            WHEN age BETWEEN 10 AND 19 THEN '10-19'
            WHEN age BETWEEN 20 AND 29 THEN '20-29'
            WHEN age BETWEEN 30 AND 39 THEN '30-39'
            WHEN age BETWEEN 40 AND 49 THEN '40-49'
            WHEN age BETWEEN 50 AND 59 THEN '50-59'
            WHEN age IS NULL THEN 'Unknown'
            ELSE '60+' 
        END AS age_group
    FROM 
        customer_info
),
transactions_by_age AS (
    SELECT 
        ag.age_group,
        DATE_TRUNC('quarter', t.date_new) AS quarter,
        COUNT(t.id_check) AS transaction_count,
        SUM(t.sum_payment) AS total_amount
    FROM 
        transaction_info t
    JOIN 
        age_groups ag ON t.id_client = ag.id_client
    WHERE 
        t.date_new BETWEEN '2015-06-01' AND '2016-06-01'
    GROUP BY 
        ag.age_group, DATE_TRUNC('quarter', t.date_new)
)
SELECT 
    age_group,
    quarter,
    AVG(total_amount) AS average_amount,
    AVG(transaction_count) AS average_transaction_count,
    (COUNT(transaction_count) * 100.0 / (SELECT COUNT(*) FROM transaction_info WHERE date_new BETWEEN '2015-06-01' AND '2016-06-01')) AS percentage_of_total
FROM 
    transactions_by_age
GROUP BY 
    age_group, quarter
ORDER BY 
    age_group, quarter;
