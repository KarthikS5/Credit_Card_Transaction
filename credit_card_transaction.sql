create database credit_card;
use credit_card;

/* Data Cleansing
ALTER TABLE  credit_card_transaction
CHANGE DATE card_date varchar(50);

DESCRIBE credit_card_transaction;

ALTER TABLE credit_card_transaction
MODIFY card_date DATE;

ALTER TABLE credit_card_transaction
change dates card_date  date;
modify city varchar(50);
modify card_type varchar(20); 
modify exp_type varchar(20);
modify gender varchar(10);


desc credit_card_transaction;
*/

# Write a query to fetch total number of each card transaction
SELECT 
    card_type, COUNT(1) AS total_transaction
FROM
    credit_card_transaction
GROUP BY 1;

# write a query to find the total number of null values are presented in card_type
SELECT 
    card_type,
    SUM(CASE WHEN card_type IS NULL THEN 1 ELSE 0 END) AS tooll
FROM
    credit_card_transaction
GROUP BY 1;

-- 1.write a query to print top 5 cities highest spent and percenatage of contribution of total credit card spends
WITH total as 
(SELECT 
    city, SUM(amount) AS total_spent
FROM
    credit_card_transaction
GROUP BY city
ORDER BY total_spent DESC
)
,total_city as 
(SELECT 
    SUM(amount) AS city_spent_total
FROM
    credit_card_transaction
)
SELECT 
    t.city,
    t.total_spent,
    ROUND((t.total_spent / tc.city_spent_total), 2) * 100 AS total_perc_city
FROM
    total t
        JOIN
    total_city tc ON 1 = 1
LIMIT 5;

# OR

SELECT 
    cc.city,
    SUM(amount) AS total_spent,
    ROUND(SUM(cc.amount) / (SELECT SUM(ct.amount) FROM credit_card_transaction ct) * 100,2) AS city_total
FROM
    credit_card_transaction cc
GROUP BY 1
ORDER BY city_total DESC , cc.city ASC
LIMIT 5;

/* 2.write a query to print highest spend month and amount spend in that month for each card type.*/

WITH date_wise AS
(SELECT 
    card_type,
    MONTHNAME(card_date) AS month,
    YEAR(card_date) AS year,
    SUM(amount) AS total_spend,
	DENSE_RANK()OVER(PARTITION BY  card_type ORDER BY SUM(amount) DESC) AS highest_rank
FROM
    credit_card_transaction
GROUP BY 1 , month , year
)
SELECT 
    card_type, month, year, total_spend
FROM
    date_wise
WHERE
    highest_rank = 1;

/* 3.write a query to print transaction details(all column from table) for each card type when its reaches a
cumulative of 100000 total spends.*/

WITH cte AS (
SELECT *,
      SUM(amount) OVER(PARTITION BY card_type ORDER BY card_date,amount) AS cumulative
FROM credit_card_transaction
), 
cte2 AS 
(SELECT *,
       DENSE_RANK()OVER(PARTITION BY  card_type ORDER BY  cumulative ) AS cum_rank
FROM cte 
WHERE cumulative>=100000
)
SELECT 
    city, card_type, cumulative
FROM
    cte2
WHERE
    cum_rank = 1;
  
/* 4.write a query to find the city which had lowest percenate spend for gold card type.*/
WITH  gold_spend_city  AS
(SELECT 
    city, SUM(amount)AS spend
FROM
    credit_card_transaction
WHERE
    card_type = 'gold'
GROUP BY city
ORDER BY spend)
,total_spend AS 
(SELECT 
    city, SUM(amount) AS spend_city
FROM
    credit_card_transaction
GROUP BY city)
SELECT 
    gc.city,
    gc.spend,
    ROUND((spend / spend_city) * 100, 2) AS perc
FROM
    gold_spend_city gc
        INNER JOIN
    total_spend ts ON gc.city = ts.city
GROUP BY 1
ORDER BY perc
LIMIT 3;

 # or 

SELECT 
    ct.city,
    SUM(ct.amount),
    SUM(ct.amount) / SUM(cc.amount) * 100 AS total_trans
FROM
    credit_card_transaction ct
        INNER JOIN
    credit_card_transaction cc ON ct.city = cc.city
WHERE
    ct.card_type = 'gold'
GROUP BY 1
ORDER BY total_trans
;
       
      --  and  for platinum
with platinum_spend_city as 
(SELECT 
    city, SUM(amount) AS spend
FROM
    credit_card_transaction
WHERE
    card_type = 'platinum'
GROUP BY city)
,total_spend as 
(SELECT 
    city, SUM(amount) AS spend_city
FROM
    credit_card_transaction
GROUP BY city)
SELECT 
    pc.city,
    pc.spend,
    ROUND((spend / spend_city) * 100, 2) AS perc
FROM
    platinum_spend_city pc
        JOIN
    total_spend ts ON pc.city = ts.city
ORDER BY perc
LIMIT 1;

-- 5.write a query to top3: city, highest_exp, lowest_exp, (ex: delhi,bills, fuel)
WITH spend_amount AS 
(SELECT 
    city AS city, exp_type AS expense, SUM(amount) AS spend
FROM
    credit_card_transaction
GROUP BY city , exp_type)
,high_low AS 
(SELECT 
    city, MAX(spend) AS highest_exp, MIN(spend) AS lowest_exp
FROM
    spend_amount
GROUP BY city
)
SELECT 
    sa.city,
    MAX(CASE
        WHEN spend = highest_exp THEN expense END) AS highest_expp,
    MIN(CASE
		WHEN spend = lowest_exp THEN expense END) AS lowest_expp
FROM
    spend_amount sa
        JOIN
    high_low hl ON sa.city = hl.city
GROUP BY sa.city
ORDER BY sa.city;


-- 6.write a query to percentage contribution by female each exp_type,
SELECT
  exp_type,
ROUND(SUM(CASE WHEN gender = 'f' THEN amount ELSE 0 END) * 100.0 / SUM(amount), 2) as percentage_contribution
FROM
  credit_card_transaction 
GROUP BY
  1;
# or

WITH female_spents AS 
(SELECT 
    exp_type, SUM(amount) AS female_spent
FROM
    credit_card_transaction
WHERE
    gender = 'f'
GROUP BY exp_type)
,total_spends AS 
(SELECT 
    exp_type, SUM(amount) AS total_spent
FROM
    credit_card_transaction
GROUP BY exp_type)
SELECT 
    fs.exp_type
    (fs.female_spent) / total_spent AS female_percantage_spent
FROM
    female_spents fs
        JOIN
    total_spends ts ON fs.exp_type = ts.exp_type
GROUP BY fs.exp_type , fs.female_spent , ts.total_spent;
 
-- 7.write a query to percentage contribution by male each exp_type,
SELECT
     exp_type,
     ROUND(SUM(CASE WHEN gender = 'm' THEN amount ELSE 0 END) * 100.0 / SUM(amount), 2) as percentage_contribution
FROM
  credit_card_transaction 
GROUP BY
  1;
  
  # or
  
WITH male_spents as 
(SELECT 
    exp_type, SUM(amount) AS male_spent
FROM
    credit_card_transaction
WHERE
    gender = 'm'
GROUP BY exp_type)
,total_spends AS 
(SELECT 
    exp_type, SUM(amount) AS total_spent
FROM
    credit_card_transaction
GROUP BY exp_type)
SELECT 
    ms.exp_type,
    ROUND((ms.male_spent / ts.total_spent), 2) * 100 AS male_percantage_spent
FROM
    male_spents ms
        JOIN
    total_spends ts ON ms.exp_type = ts.exp_type
GROUP BY ms.exp_type , ms.male_spent , ts.total_spent;


-- 8.which card and expense type combination saw highest month over month growth in jan-2014
with expense as 
(SELECT 
    card_type,
    exp_type,
    MONTHNAME(card_date) AS month,
    YEAR(card_date) AS year,
    SUM(amount) AS highest_spent
FROM
    credit_card_transaction
GROUP BY card_type , exp_type , month , year)
, month_year as
(select *,
       lag(highest_spent,1)over(partition by card_type,exp_type  order by  year, month desc) as prv_month_year
from expense
	)
SELECT 
    card_type,
    exp_type,
    month,
    year,
    100 * (highest_spent - prv_month_year) / prv_month_year AS growth
FROM
    month_year
WHERE
    year = 2014 AND month = 'january'
GROUP BY card_type , exp_type , month , year
ORDER BY growth DESC
LIMIT 1;

-- 9. During weekend which city has highest total spends to no_of_transaction ratio
SELECT 
    city,
    SUM(amount) AS total_spents,
    COUNT(1) AS no_of_tranasaction,
    SUM(amount) / COUNT(1) AS ratio
FROM
    credit_card_transaction
WHERE
    DAYNAME(card_date) IN ('sunday' , 'saturday')
GROUP BY 1
ORDER BY ratio DESC
LIMIT 1;

-- 10.which city tooks least number of days to reaches its 500th transaction after first transaction is that city
WITH cte as(     
SELECT *,
   ROW_NUMBER()OVER(PARTITION BY city ORDER BY card_date) as num_trans,
   MIN(card_date)OVER(PARTITION BY city) AS first_trans
FROM credit_card_transaction
)
SELECT 
    city,
    card_date,
    card_type,
    DATEDIFF(card_date, first_trans) AS minimum_500_transaction
FROM
    cte
WHERE
    num_trans = 500
ORDER BY minimum_500_transaction 
LIMIT 1
;

-- 11. write a query to fetch monthly transaction percantege for every year transaction done in bangalore region.
WITH cte AS  (
	SELECT 
    EXTRACT(MONTH FROM card_date) AS months,
    EXTRACT(YEAR FROM card_date) AS year,
    COUNT(1) AS no_of_trans
FROM
    credit_card_transaction
WHERE
    city LIKE 'bengaluru%'
GROUP BY 1 , 2
ORDER BY year , months
) 
,cte2 AS (
   SELECT 
    year, SUM(no_of_trans) AS total
FROM
    cte
GROUP BY year
)
SELECT 
    cte.months,
    cte2.year,
    ROUND((no_of_trans / total) * 100, 2) AS perc_trans
FROM
    cte2
        JOIN
    cte ON cte2.year = cte.year
;

# Write a query to fetch previous year sales
with cte as(
SELECT 
    MAX(card_date) AS max_date
FROM
    credit_card_transaction
)
SELECT 
    card_type, SUM(amount) as total_spend
FROM
    credit_card_transaction
        INNER JOIN
    cte
WHERE
    card_date >= DATE_SUB(max_date, INTERVAL 1  YEAR)
GROUP BY 1;

# Write a query to fetch 7 day rolling average
SELECT 
    city,
    card_date,
	amount, 
	AVG(AMOUNT)OVER(PARTITION BY city ORDER BY card_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW ) AS rolling_avg
FROM credit_card_transaction
GROUP BY  1,2,3;



# write a query to fetch the previous 7 days total spend
with cte as(
SELECT 
    amount,
    card_type,
    EXTRACT(MONTH FROM card_date) AS months,
    EXTRACT(YEAR FROM card_date) AS years
FROM
    credit_card_transaction
)
SELECT card_type,sum(amount)
FROM credit_card_transaction
WHERE card_date >= month((select max(card_date)from credit_card_transaction) - 7 )  
group by 1;





