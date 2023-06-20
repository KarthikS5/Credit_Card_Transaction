create database credit_card;
use credit_card;


select * from credit_card_transaction;


alter table  credit_card_transaction
change date card_date varchar(50);

select*,
avg(amount) over(partition by city order by date) as row_num
from credit_card_transaction
where card_type='gold';



-- 1.write a query to print top 5 cities highest spent and percenatage of contribution of total credit card spends
wITH total as (select city , sum(amount) as total_spent 
from credit_card_transaction
group by city
order by total_spent desc)
, total_city as (select sum(amount)as city_spent_total
from credit_card_transaction)
select t.city,t.total_spent, round(avg(t.total_spent/tc.city_spent_total),2)*100 as total_perc_city
 from total t join total_city tc
on 1=1
group by 1,2
 limit 5;



-- 2.write a query to print highest spend month and amount spend in that month for each card type

with date_wise as(select  card_type, monthname(card_date) as month,year(card_date) AS year,sum(amount) as total_spend 
from credit_card_transaction
group by 1,month,year
)
  , ranking as (select *,
dense_rank()over(partition by card_type order by total_spend desc ) as highest_rank
from date_wise)
select card_type,month,year,total_spend
from ranking
where highest_rank=1;

-- 3.write a query to print transaction details(all column from table) for each card type when its reaches a
-- cumulative of 100000 total spends

with cte as (select *,
sum(amount) over(partition by card_type order by card_date, amount) as cumulative
from credit_card_transaction)
, cte2 as (select *,
dense_rank()over(partition by card_type order by cumulative desc )as dn
from cte 
 where cumulative>=100000)
 select * from cte2
  where dn=1;
  
-- 4.write a query to find the city which had lowest percenate spend for gold card type
with gold_spend_city as (select city, sum(amount) as spend
 from credit_card_transaction 
         where card_type='gold'
         group by city)
 ,total_spend as (select city, sum(amount) as spend_city
   from credit_card_transaction
         group by city)
select gc.city,gc.spend,round(avg(spend/spend_city)*100,2) as perc
from gold_spend_city gc
     join total_spend ts
     on gc.city=ts.city
       group by gc.city
       order by perc 
       limit 1;
       
      --  and  for platinum
	with platinum_spend_city as (select city, sum(amount) as spend
 from credit_card_transaction 
         where card_type='platinum'
         group by city)
 ,total_spend as (select city, sum(amount) as spend_city
   from credit_card_transaction
         group by city)
select pc.city,pc.spend,round(avg(spend/spend_city)*100,2) as perc
from platinum_spend_city pc
     join total_spend ts
     on pc.city=ts.city
       group by pc.city
       order by perc 
       limit 1;

-- 5.write a query to top3: city, highest_exp, lowest_exp, (ex: delhi,bills, fuel)

with spend_amount as 
    (select city as city,exp_type as expense, sum(amount) as spend 
      from credit_card_transaction
      group by  city,exp_type)
, high_low as (select city,
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

with female_spents as (select exp_type, sum(amount) as female_spent 
from credit_card_transaction 
      where gender ='f'
	  group by exp_type)
 ,total_spends as (select exp_type,sum(amount) as total_spent  
  from credit_card_transaction
       group by exp_type)
  select fs.exp_type, fs.female_spent,ts.total_spent,
        round(avg(fs.female_spent/ts.total_spent),2)*100  as female_percantage_spent
  from female_spents fs join total_spends ts
        on fs.exp_type=ts.exp_type
         group by fs.exp_type, fs.female_spent,ts.total_spent;
 
-- 7.write a query to percentage contribution by male each exp_type,

with male_spents as (select exp_type, sum(amount) as male_spent 
from credit_card_transaction 
where gender ='m'
group by exp_type)
 , total_spends as (select exp_type,sum(amount) as total_spent  
       from credit_card_transaction
       group by exp_type)
  select ms.exp_type, ms.male_spent,ts.total_spent,
        round(avg(ms.male_spent/ts.total_spent),2)*100  as male_percantage_spent
  from male_spents ms join total_spends ts
        on ms.exp_type=ts.exp_type
         group by ms.exp_type, ms.male_spent,ts.total_spent;
 
 -- 8.which card and expense type combination saw highest month over month growth in jan-2014
with expense as (select card_type , exp_type , monthname(card_date) as month,year(card_date)as year, sum(amount) as highest_spent
from credit_card_transaction
group by card_type, exp_type,month,year)
 , month_year as
 (select *,
lag(highest_spent,1)over(partition by card_type,exp_type  order by  year, month desc) as prv_month_year
from expense)
,cte as(select card_type, exp_type,month,year,
100*(highest_spent-prv_month_year)/prv_month_year as growth
from month_year
where year=2014 and month='january'
group by card_type, exp_type,month,year
)
select * from cte 
order by growth desc
limit 1; 


select city, sum(amount) as total_spents,
count(1) as no_of_tranasaction, extract(week from card_date) as week,
 sum(amount)/count(1) as ratio
from credit_card_transaction
--  where extract(week from card_date) in ('1','7')
group by 1,extract(week from card_date)
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
             limit 1
