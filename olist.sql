/*
Data source: kaggle
Data description: This data set is the transaction data of Brazilian e-commerce Olist, providing nearly 100,000 items from 16 to 18 years on the platform  transaction records, the data set contains 9 data tables
*/


/*
1. Analysis framework:
	1.1 Traffic indicators: number of active users (DAU, MAU, time period)
	1.2 Operational indicators: GMV (quarter, month), ARPU (quarter, month), number of orders (day, month, period)
	1.3 RFM segmentation : user categories at each level (popularity index = amount + evaluation score)

*/



--1.load datasets
	--1.1 create table
drop table If exists olist_customer;

create table olist_customer
( customer_id varchar(50)
, customer_unique_id varchar(50)
, customer_zip_code_prefix int
, customer_city varchar(50)
, customer_state varchar(5)
);

drop table If exists olist_geo;
create table olist_geolocation_dataset 
( geolocation_zip_code_prefix  int
, geolocation_at decimal(14,12) 
, geolocation_lng decimal(14,12)
, geolocation_city varchar(30)
, geolocation_state varchar(5)
);

drop table If exists olist_order_items;
create table olist_order_items
( order_id varchar(50)
, order_item_id int
, product_id varchar(50)
, seller_id varchar (100)
, shipping_limit_date datetime
, price float
, freight_value float
);

create table olist_order_payments 
(  order_id varchar(50)
, payment_sequential int
, payment_type varchar(50)
, payment_installments int
, payment_value float
);

drop table if exists olist_order_reviews_dataset;
create table olist_order_reviews
( review_id varchar(50)
, order_id varchar(50)
, review_score int
, review_comment_title varchar(50)
, review_comment_message varchar(500)
, review_creation_date datetime
, review_answer_datetime datetime
);

create table olist_orders 
( order_id varchar(50)
, customer_id varchar(50)
, order_status varchar(20)
, order_purchase_datetime datetime
, order_approved_at datetime
, order_delivered_carrier_date datetime
, order_delivered_customer_date datetime
, order_estimated_delivery_date datetime
);


create table olist_products 
( product_id varchar(50)
, product_category_name varchar(50)
, product_name_lenght int
, product_description_lenght int
, product_photos_qty int
, product_weight_g int
, product_length_cm int
, product_height_cm int
, product_width_cm int
);

create table olist_sellers 
( seller_id varchar(50)
, seller_zip_code_prefix int
, seller_city varchar(50)
, seller_state varchar(5)
);
 
 create table product_category_name 
( product_category_name  varchar(50)
, product_category_name_english varchar(50)
);


--1.2 insert data
bulk insert olist_customer from "D:\Data\olist\archive\olist_customers_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

bulk insert olist_geolocation_dataset from "D:\Data\olist\archive\olist_geolocation_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

bulk insert olist_order_items from "D:\Data\olist\archive\olist_order_items_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

bulk insert olist_order_payments from "D:\Data\olist\archive\olist_order_payments_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

bulk insert olist_order_reviews from "D:\Data\olist\archive\olist_order_reviews_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

bulk insert olist_orders from "D:\Data\olist\archive\olist_orders_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

bulk insert olist_products from "D:\Data\olist\archive\olist_products_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

bulk insert olist_sellers from "D:\Data\olist\archive\olist_sellers_dataset.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);  -- spare ',' exist in column, replaced by | in csv file. 

bulk insert product_category_name from "D:\Data\olist\archive\product_category_name_translation.csv"
with(
FIRSTROW = 2,
fieldterminator = ',',
ROWTERMINATOR  = '0x0a'
);

--1.3 check table
select top 100 * from[dbo].[olist_customer];
select top 100 * from[dbo].[olist_geolocation_dataset];
select top 100 * from[dbo].[olist_order_items];
select top 100 * from[dbo].[olist_order_payments];
select top 100 * from[dbo].[olist_order_reviews];
select top 100 * from[dbo].[olist_orders];
select top 100 * from[dbo].[olist_products];
select top 100 * from[dbo].[olist_sellers];
select top 100 * from [dbo].[product_category_name];

--2. Business Objective:
/*	
	2.1 Analyze the overall operating status of Olist through 4 dimensions of users, merchants, products, and sales
	2.2 Identify problem, cause analysis and present suggestion.
	2.3 Identify abnormal metrics, analyze the cause. 
	2.4 Provide directionl, goals and suggestions.
*/

--3.3 Analytical architacture
/* 3.1 User: 
	 3.1.1 behavior:
		A. payment
		B. install payment
		c. order number
		d. payment time
	 3.1.2 RFM segmentation:
	 3.1.3 Regional distribution 

	3.2 Merchants: 
	 3.2.1 reginal distribution
	 3.2.2 transaction volume
	 3.2.3 avg score
	 3.2.4 delivery time
   
   3.3 Products
    3.3.1 SKU
	3.3.2 Products contribution rate 
	3.3.3 sales amount
	3.3.4 sales volume
   3.4 Sales
    3.4.1 total sales amount
	3.4.2 total sales volume
	3.4.3 avg transaction value(ATV)
	3.4.4 regional: volume & amount
	3.4.5 time period: year, month, season volume &amount

*/
--2. data cleanup

	--2.1 check null and replace
select * from olist_customer
where customer_id is null  or customer_unique_id is null or customer_zip_code_prefix is null or customer_city is null or customer_state is null; ---no null value

select * from[dbo].[olist_geolocation_dataset]
where geolocation_zip_code_prefix is null or geolocation_lat is null or geolocation_lng is null or geolocation_city is null or geolocation_state is null;  ---no null value

select  * from[dbo].[olist_order_items]
where order_id is null or order_item_id is null or product_id is null or seller_id is null or shipping_limit_date is null or price is null or freight_value is null; --no null value

select  * from[dbo].[olist_order_payments]
where order_id is null or payment_sequential is null or payment_type is null or payment_installments is null or payment_value is null; --no null value

select  * from[dbo].[olist_order_reviews]
where review_id is null or order_id is null or review_score is null or review_comment_title is null or review_comment_message is null or review_creation_date is null or review_answer_datetime is null ; 

--title and message have null value, set it to 0

update [dbo].[olist_order_reviews] 
set review_comment_title = 0
where review_comment_title is null;

update [dbo].[olist_order_reviews] 
set review_comment_message = 0
where review_comment_message is null;

--check 
select  * from[dbo].[olist_order_reviews]
where review_id is null or order_id is null or review_score is null or review_comment_title is null or review_comment_message is null or review_creation_date is null or review_answer_datetime is null 

select  * from[dbo].[olist_orders]
where order_id is null or customer_id is null or order_status is null or order_purchase_datetime is null 
		or order_approved_at is null or order_delivered_carrier_date is null or order_delivered_customer_date is null or order_estimated_delivery_date is null;
---order_approved_at, order_delivered_carrier_date, order_delivered_customer_date have null value, replace by 0

update [dbo].[olist_orders]
set order_approved_at= 0
where order_approved_at is null;

update [dbo].[olist_orders]
set order_delivered_carrier_date= 0
where order_delivered_carrier_date is null;

update [dbo].[olist_orders]
set order_delivered_customer_date= 0
where order_delivered_customer_date is null;

--check
select  * from[dbo].[olist_orders]
where order_id is null or customer_id is null or order_status is null or order_purchase_datetime is null 
		or order_approved_at is null or order_delivered_carrier_date is null or order_delivered_customer_date is null or order_estimated_delivery_date is null;

select  * from[dbo].[olist_products]
where product_id is null or product_category_name is null or product_description_lenght is null or product_name_lenght is null or product_photos_qty is null
	or product_weight_g is null or product_height_cm is null or product_width_cm is null;

--product_category_name,product_description_lenght,product_name_lenght and product_photos_qty are null value, delete from talbe

alter table [dbo].[olist_products]
drop column  product_category_name;

alter table [dbo].[olist_products]
drop column product_description_lenght;

alter table [dbo].[olist_products]
drop column product_name_lenght;

alter table [dbo].[olist_products]
drop column product_photos_qty;

--check
select  * from[dbo].[olist_products]
where product_id is null or product_weight_g is null or product_height_cm is null or product_width_cm is null; 

/*
product_id	product_weight_g	product_length_cm	product_height_cm	product_width_cm
"09ff539a621711667c43eba6a3bd8466"	NULL	NULL	NULL	NULL
"5eb564652db742ff8f28759cd8d2652a"	NULL	NULL	NULL	NULL
*/--- delete

delete from [dbo].[olist_products]
where product_id ='"09ff539a621711667c43eba6a3bd8466"' or product_id = '"5eb564652db742ff8f28759cd8d2652a"';

--check
select  * from[dbo].[olist_products]
where product_id is null or product_weight_g is null or product_height_cm is null or product_width_cm is null; 


select * from[dbo].[olist_sellers]
where seller_id is null or seller_zip_code_prefix is null or seller_city is null or seller_state is null;-- no null value

select  * from [dbo].[product_category_name]
where product_category_name is null or product_category_name_english is null; --no null value

--2.2 check duplication

-- orders table
select order_id 
from olist_orders
group by order_id
having count(*)>1;  --no duplication

--item table
SELECT order_id  
from olist_order_items
GROUP BY order_id, order_item_id
HAVING count(*)>1;   -- no duplication

--review table
SELECT review_id,order_id
from olist_order_reviews
group by review_id,order_id
HAVING count(*)>1;  -- no duplication

-- product table
SELECT product_id 
from olist_products
GROUP BY product_id
HAVING count(*)>1;  -- no duplication

--category table
SELECT product_category_name_english 
from [product_category_name]
GROUP BY product_category_name_english
HAVING count(*)>1;						-- no duplication

/* no duplication on table in use for the analysis */

-- Extract time data, create time table

Select * into  order_time
from(
SELECT  order_id
		, customer_id
		, datepart (yy, order_purchase_datetime) as y
		, datepart (qq, order_purchase_datetime) as q
		, datepart (mm, order_purchase_datetime) as m
		, datepart (dd, order_purchase_datetime) as d
		, datepart (hh, order_purchase_datetime) as h
from olist_orders
where order_purchase_datetime not like '2016-09-%' 		-- filter months with  abnormal data
and order_purchase_datetime not like '2016-12-%'
and order_purchase_datetime not like '2018-09-%'
and order_purchase_datetime not like '2018-10-%')a ;

--check 
Select top 1000 *
from order_time;

-- calculate the amount of each order

CREATE view total_order_value as
	select order_id,product_id,seller_id,
		   (price*count_num)+(freight_value*count_num) as order_value
	from (
		select a.*, b. count_num
		from [dbo].[olist_order_items] a
		left join 
		(select  order_id, count(*) count_num
		from [olist_order_items]
		group by order_id) b
		on  a.order_id = b.order_id
		) c;

--check 
select top 100 * 
from total_order_value;

--calcuate the time and amount of each order for GMV using

CREATE view order_detail as
	select a.order_id,product_id,seller_id,
		   customer_id,
		   round(order_value,2) as order_value,
		   y,q,m,d,h
	from total_order_value a
	inner join order_time b
	on a.order_id=b.order_id;

--check 
select top 100 *
from order_detail;

--3. Data analysis

	--3.1 User stickiness analysis
	/*
	User stickiness is  calculated by the formula of DAU / MAU. 
	The closer the ratio is to 1, the higher the user activity is. When the ratio is lower than 0.2, the spread and interaction of the application will be weak.
	In the dataset, each user has only one order record,so here the number of orders and the number of active users as the same data
	*/

-- time distributtion of active user

-- DAU
select d dates,count(DISTINCT customer_id) DAU
from order_detail
group by d
order by d;

-- MAU
select y years, m months,count(DISTINCT customer_id) MAU
from order_detail
group by y, m
order by y, m;



-- user stickiness trend analysis(dau/mau)
-- create view of average DAU 

CREATE view avg_DAU as
	select y years, m months
		,round(avg(dau),2) avg_DAU
	from (select y,m,d
				,count(distinct customer_id) dau
		  from order_detail
		  group by y, m, d
		 ) t
	group by y, m;

select *
from avg_dau;

-- user stickness monthly

CREATE view user_viscocity as
	select years, months
	, avg_dau
	, b.mau
	, concat( cast(avg_dau*1.0/b.MAU *100 as decimal(10,2)), '%') Stickness
	from avg_dau a 
	join (select y, m ,count(DISTINCT customer_id) MAU
			from order_detail 
			group by y, m
		) b
	on a.years = b.y and a.months = b.m;

--check
select *
from user_viscocity
order by years, months;  --abnormal in 2016-12 and 2018-9

--2. GMV analysis (Gross Merchandise Volume)

--quarter GMV
select y years,q quarters,sum(order_value) Q_GMV
from order_detail
group by y,q
order by y,q;
 
-- monthly GMV

select y years, m months,sum(order_value) m_GMV
from order_detail
group by y,m
order by y,m;




--3. ARPU analysis(Average Revenue Per User)

/*
Average Revenue Per User, The higher the proportion of mid-to-high-end customers is, the higher the ARPU value will be
*/

-- quartly ARPU  = GMV / QAU
select y years ,q quarters,
       round((sum(order_value)/count(DISTINCT customer_id)),2) Q_ARPU
from order_detail
group by y,q
order by y,q;
 
-- monthly ARPU  = GMV / MAU
select y years, m months,
       round((sum(order_value)/count(DISTINCT customer_id)),2) M_ARPU
from order_detail
group by y,m
order by y,m;


--4. RFM segmentation

-- frequency
SELECT customer_id, count(*) F
from order_detail
GROUP BY customer_id
HAVING count(*)>1;   ---due to all customer have one transaction, So Frequency default as low
 
-- recency
--create view for recency of each customer

CREATE VIEW recency_per_user as
	SELECT customer_id,
		   DATEDIFF(d, d, (select max(d) from order_detail)) as Recency
	FROM order_detail;

	select * from order_detail;

-- Assign R value

CREATE VIEW Recency AS
	SELECT customer_id, Recency,
			(CASE WHEN recency >(SELECT AVG(recency) from recency_per_user ) 
                    THEN 1 ELSE 0 END) R
FROM recency_per_user;
 
--monetary
--create view for monetary of each customer
CREATE VIEW ordervalue_per_user AS
select customer_id,sum(order_value) sum_value
from order_detail
group by customer_id;
 
-- assign M value
CREATE VIEW Monetary AS
select customer_id,(case when (select avg(sum_value) from ordervalue_per_user) < sum_value
                    then 1 else 0 end) M
from ordervalue_per_user;
 
-- labeling segmentation
CREATE VIEW RFM AS
SELECT Recency.customer_id, 
		(CASE WHEN R=1 AND M=1 THEN 'Champions '
			  WHEN R=0 AND M=1 THEN 'Potential Loyalists'
			  WHEN R=1 AND M=0 THEN 'Need Attention'
			  WHEN R=0 AND M=0 THEN 'Lost customer'
			  ELSE 'else' 
		 END) AS segmentation
FROM Recency 
INNER JOIN Monetary 
ON Recency.customer_id=Monetary.customer_id; 

--5. Popular products to each segmentation
 /*
	popular index =0.7 * sales  + 0.3 * reviews *10000 (10000 for balancing the gad between sales volum and review)
 */

-- check number of each segmentation
SELECT segmentation ,count(*) num 
from rfm
GROUP BY segmentation
ORDER BY segmentation;
 
-- popular category of each products to Champions

CREATE VIEW popular_cat_to_champions as
	SELECT e.product_category_name_english as pro_cat,
			SUM(a.order_value) as value_amount,
			ROUND(AVG(c.review_score),2) as  reviews,
			(0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),2)) as popular_index,
			rank() over(ORDER BY (0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),7)) DESC) as popular_rank
	FROM order_detail a
	INNER JOIN (SELECT customer_id from rfm WHERE segmentation='Champions' ) as b ON a.customer_id=b.customer_id
	LEFT JOIN [dbo].[olist_order_reviews] c ON a.order_id=c.order_id
	LEFT JOIN [dbo].[olist_products] d ON a.product_id=d.product_id
	LEFT JOIN [dbo].[product_category_name] e ON d.product_category_name=e.product_category_name
	GROUP BY e.product_category_name_english;
 
 

--popular category of each products to Potential Loyalists

CREATE VIEW popular_cat_to_potential as
	SELECT e.product_category_name_english as pro_cat,
			SUM(a.order_value) as value_amount,
			ROUND(AVG(c.review_score),2) as  reviews,
			(0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),2)) as popular_index,
			rank() over(ORDER BY (0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),7)) DESC) as popular_rank
	FROM order_detail a
	INNER JOIN (SELECT customer_id from rfm WHERE segmentation='Potential Loyalists' ) as b ON a.customer_id=b.customer_id
	LEFT JOIN [dbo].[olist_order_reviews] c ON a.order_id=c.order_id
	LEFT JOIN [dbo].[olist_products] d ON a.product_id=d.product_id
	LEFT JOIN [dbo].[product_category_name] e ON d.product_category_name=e.product_category_name
	GROUP BY e.product_category_name_english;
 
 
--popular category of each products to Need Attention

CREATE VIEW popular_cat_to_attention as
	SELECT e.product_category_name_english as pro_cat,
			SUM(a.order_value) as value_amount,
			ROUND(AVG(c.review_score),2) as  reviews,
			(0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),2)) as popular_index,
			rank() over(ORDER BY (0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),7)) DESC) as popular_rank
	FROM order_detail a
	INNER JOIN (SELECT customer_id from rfm WHERE segmentation='Need Attention' ) as b ON a.customer_id=b.customer_id
	LEFT JOIN [dbo].[olist_order_reviews] c ON a.order_id=c.order_id
	LEFT JOIN [dbo].[olist_products] d ON a.product_id=d.product_id
	LEFT JOIN [dbo].[product_category_name] e ON d.product_category_name=e.product_category_name
	GROUP BY e.product_category_name_english;
 
 
--popular category of each products to lost customer

CREATE VIEW popular_cat_to_lost as
	SELECT e.product_category_name_english as pro_cat,
			SUM(a.order_value) as value_amount,
			ROUND(AVG(c.review_score),2) as  reviews,
			(0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),2)) as popular_index,
			rank() over(ORDER BY (0.7*SUM(a.order_value)+0.3*10000*ROUND(AVG(c.review_score),7)) DESC) as popular_rank
	FROM order_detail a
	INNER JOIN (SELECT customer_id from rfm WHERE segmentation='lost customer' ) as b ON a.customer_id=b.customer_id
	LEFT JOIN [dbo].[olist_order_reviews] c ON a.order_id=c.order_id
	LEFT JOIN [dbo].[olist_products] d ON a.product_id=d.product_id
	LEFT JOIN [dbo].[product_category_name] e ON d.product_category_name=e.product_category_name
	GROUP BY e.product_category_name_english;