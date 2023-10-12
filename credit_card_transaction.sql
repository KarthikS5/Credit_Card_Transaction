create database credit_card;
use credit_card;

-- DATA CLEAN AND PREPROCESSING
-- ALTER TABLE  credit_card_transaction
-- CHANGE DATE card_date varchar(50);

-- DESCRIBE credit_card_transaction;

-- ALTER TABLE credit_card_transaction
-- MODIFY card_date DATE;

-- ALTER TABLE credit_card_transaction
-- change dates card_date  date;

-- ALTER TABLE credit_card_transaction
-- modify city varchar(50);

-- 1.write a query to print top 5 cities highest spent and percenatage of contribution of total credit card spends
create view top_5_city_spent as 
WITH total as 
        (select city , sum(amount) as total_spent
               from credit_card_transaction
			   group by city
               order by total_spent desc)
,total_city as 
		(select sum(amount)as city_spent_total
		 from credit_card_transaction)
select t.city,t.total_spent, 
round((t.total_spent/tc.city_spent_total),2)*100 as total_perc_city
from total t join total_city tc
on 1=1
group by 1,2
limit 5;
    select * from top_5_city_spent;



-- 2.write a query to print highest spend month and amount spend in that month for each card type
CREATE VIEW  highest_spend_amt_mnt AS
WITH date_wise AS
    (SELECT 
    card_type,
    MONTHNAME(card_date) AS month,
    YEAR(card_date) AS year,
    SUM(amount) AS total_spend
FROM
    credit_card_transaction
GROUP BY 1 , month , year
)
,ranking AS 
     (select *,
     dense_rank()over(partition by card_type order by total_spend desc) AS highest_rank
     from date_wise
)
SELECT 
    card_type, month, year, total_spend
FROM
    ranking
WHERE
    highest_rank = 1;

-- 3.write a query to print transaction details(all column from table) for each card type when its reaches a
-- cumulative of 100000 total spends

WITH cte AS 
      (select *,
      sum(amount) over(partition by card_type order by card_date, amount) as cumulative
      from credit_card_transaction)
, cte2 AS 
      (select *,
       dense_rank()over(partition by card_type order by cumulative  )as dn
       from cte 
       where cumulative>=100000)
select city,card_type,cumulative from cte2
where dn=1;
  
-- 4.write a query to find the city which had lowest percenate spend for gold card type
WITH  gold_spend_city  AS
        (select city, sum(amount) as spend
         from credit_card_transaction 
         where card_type='gold'
         group by city)
,total_spend AS 
       (select city, sum(amount) as spend_city
       from credit_card_transaction
		group by city)
select gc.city,gc.spend,round((spend/spend_city)*100 ,2)as perc
from gold_spend_city gc inner join total_spend ts
on gc.city=ts.city
group by gc.city
order by perc 
limit 3;
       
      --  and  for platinum
with platinum_spend_city as 
        (select city, sum(amount) as spend
        from credit_card_transaction 
         where card_type='platinum'
         group by city)
,total_spend as 
       (select city, sum(amount) as spend_city
        from credit_card_transaction
		group by city)
select pc.city,pc.spend,round(avg(spend/spend_city)*100,2) as perc
from platinum_spend_city pc join total_spend ts
on pc.city=ts.city
group by pc.city
order by perc 
limit 1;

-- 5.write a query to top3: city, highest_exp, lowest_exp, (ex: delhi,bills, fuel)
WITH spend_amount AS 
     (select city as city,exp_type as expense, sum(amount) as spend 
      from credit_card_transaction
      group by  city,exp_type)
,high_low AS (select city,
     max(spend) as highest_exp,
     min(spend) as lowest_exp
 from spend_amount
     group by city)
select sa.city,
       max(case when spend=highest_exp then expense end) as highest_expp,
       min(case when spend=lowest_exp then expense end )as lowest_expp 
from spend_amount sa join high_low hl
       on sa.city=hl.city
       group by sa.city
       order by sa.city;


-- 6.write a query to percentage contribution by female each exp_type,

WITH female_spents AS 
      (select exp_type, sum(amount) as female_spent 
      from credit_card_transaction 
      where gender ='f'
	  group by exp_type)
,total_spends AS 
      (select exp_type,sum(amount) as total_spent  
       from credit_card_transaction
       group by exp_type)
select fs.exp_type, fs.female_spent,ts.total_spent,
	   round(avg(fs.female_spent/ts.total_spent),2)*100  as female_percantage_spent
from female_spents fs join total_spends ts
	   on fs.exp_type=ts.exp_type
	   group by fs.exp_type, fs.female_spent,ts.total_spent;
 
-- 7.write a query to percentage contribution by male each exp_type,

WITH male_spents as 
    (select exp_type, sum(amount) as male_spent 
    from credit_card_transaction 
    where gender ='m'
    group by exp_type)
,total_spends AS 
      (select exp_type,sum(amount) as total_spent  
       from credit_card_transaction
       group by exp_type)
select ms.exp_type, ms.male_spent,ts.total_spent,
        round(avg(ms.male_spent/ts.total_spent),2)*100  as male_percantage_spent
from male_spents ms join total_spends ts
        on ms.exp_type=ts.exp_type
		group by ms.exp_type, ms.male_spent,ts.total_spent;
 
 -- 8.which card and expense type combination saw highest month over month growth in jan-2014
with expense as (select card_type ,exp_type,monthname(card_date) as month,year(card_date)as year,
     sum(amount) as highest_spent
from credit_card_transaction
group by card_type, exp_type,month,year)
, month_year as
       (select *,
       lag(highest_spent,1)over(partition by card_type,exp_type  order by  year, month desc) as prv_month_year
       from expense)
,cte as
      (select card_type, exp_type,month,year,
       100*(highest_spent-prv_month_year)/prv_month_year as growth
       from month_year
       where year=2014 and month='january'
       group by card_type, exp_type,month,year
)
select * from cte 
order by growth desc
limit 1; 

-- 9. During weekend which city has highest total spends to no_of_transaction ratio
select city, sum(amount) as total_spents,
      count(1) as no_of_tranasaction,
      sum(amount)/count(1) as ratio
from credit_card_transaction
where dayofweek( card_date) in ('1','7')
     group by 1
	 order by ratio desc
     limit 1;

-- 10.which city tooks least number of days to reaches its 500th transaction after first transaction is that city

select  city , card_date, datediff(min(card_Date), min(first_transaction))as minimum_days 
       from 
       (select city, card_date,
	          row_number()over(partition by city order by card_date  ) as transaction,
			  min(card_date) over(partition by city) as first_transaction
	   from credit_card_transaction)a 
             where transaction=500
             group by city,card_date
             order by minimum_days
             limit 1;
       
       
-- 11. Gender wise total amount spent on each card_type
WITH cte AS 
    (select gender,card_type, sum(amount) as exp_by_gender from credit_card_transaction 
     group by 1,2) 
,cte2 AS 
(
select sum(amount) as total_exp from credit_card_transaction
)
select cte.gender,cte.card_type, (exp_by_gender/total_exp)*100 as perce_exp
 from cte2 left join cte 
 on cte.gender=cte.gender
;


-- 12. write a query to fetch monthly transaction percantege for every year transaction done for region bangalore
WITH cte AS  (
	 select  extract(month from card_date)as months,extract(year from card_date)as year, 
     count(card_type) as no_of_trans 
	 from credit_card_transaction
	 where  city like 'bengaluru%' 
	 group by 1,2
	 order by year,months 
) 
,cte2 AS (
      select  year,sum(no_of_trans) as total from cte 
      group by  year
)
select cte.months ,cte2.year, round(avg(no_of_trans/total)*100,2) as perc_trans
from cte2 join  cte
on cte2.year=cte.year
group by months,year;

-- 13. write a query to find no_of-transaction growth in every month of year fro bengalore region
WITH cte AS 
       (select extract(month from card_date) as month,extract(year from card_date) as year,
        count(card_type) as no_of_transaction,
		lag(count(card_type),1,0) over(partition by extract(year from card_date) 
                 order by extract(month from card_date),extract(year from card_date)) as prv_year_tran
		from credit_card_transaction
        where city like "beng%"
        group by year,month
)
select month,year, 
round(avg(no_of_transaction-prv_year_tran)/no_of_transaction,2)*100 as perc_of_transaction
from cte
group by month,year;



