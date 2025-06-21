CREATE DATABASE sales_db;

USE sales_db;

-----creating table schemas to import the data using bulk query ----

CREATE TABLE accounts (account VARCHAR(MAX),	
                         sector VARCHAR(MAX), 	
                         year_established VARCHAR(MAX), 	
                         revenue VARCHAR(MAX), 
						 employees VARCHAR(MAX),	
                         office_location VARCHAR(MAX),	
                         subsidiary_of VARCHAR(MAX)
 );


CREATE TABLE  products(product VARCHAR(MAX),	
                        series VARCHAR(MAX), 	
						sales_price VARCHAR(MAX)
 );


CREATE TABLE  sales_pipeline (opportunity_id VARCHAR(MAX),	
                               sales_agent VARCHAR(MAX),	
                               product VARCHAR(MAX),	
                               account VARCHAR(MAX),	
                               deal_stage VARCHAR(MAX),	
                               engage_date VARCHAR(MAX),	
                               close_date VARCHAR(MAX), 
                               close_value VARCHAR(MAX)
 );


CREATE TABLE  sales_teams (sales_agent VARCHAR(MAX),	
                            manager VARCHAR(MAX), 
                            regional_office VARCHAR(MAX)
 );


 -----Inserting the data using bulk insert query -------

 BULK INSERT accounts 
 FROM 'C:\Users\Chetan Vaishnav\Downloads\accounts.csv'
   WITH (
           FIELDTERMINATOR = ',',
               ROWTERMINATOR = '\n',
                      FIRSTROW = 2 , MAXERRORS = 20
);


 BULK INSERT products 
 FROM 'C:\Users\Chetan Vaishnav\Downloads\products (3).csv'
   WITH (
           FIELDTERMINATOR = ',',
               ROWTERMINATOR = '\n',
                      FIRSTROW = 2 , MAXERRORS = 20
);



BULK INSERT sales_pipeline 
 FROM 'C:\Users\Chetan Vaishnav\Downloads\sales_pipeline.csv'
   WITH (
           FIELDTERMINATOR = ',',
               ROWTERMINATOR = '\n',
                      FIRSTROW = 2 , MAXERRORS = 20
);


BULK INSERT sales_teams
 FROM 'C:\Users\Chetan Vaishnav\Downloads\sales_teams.csv'
   WITH (
           FIELDTERMINATOR = ',',
               ROWTERMINATOR = '\n',
                      FIRSTROW = 2 , MAXERRORS = 20
);



-------Now we will do some changes in the table schemas------


---accounts table-----

SELECT * FROM accounts;
SELECT 
  COLUMN_NAME, 
  DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'accounts';

ALTER TABLE  accounts
ALTER COLUMN revenue FLOAT;

ALTER TABLE accounts 
ALTER COLUMN year_established SMALLINT ;


ALTER TABLE accounts
ALTER COLUMN employees INT;

---- products table -----

SELECT * products;
SELECT 
  COLUMN_NAME, 
  DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'products';

ALTER TABLE products 
ALTER COLUMN sales_price INT

SELECT * FROM products ;


----sales_pipeline -----

Select * from sales_pipeline ;
SELECT 
  COLUMN_NAME, 
  DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'sales_pipeline';

ALTER TABLE sales_pipeline
ALTER COLUMN engage_date DATE;

ALTER TABLE sales_pipeline
ALTER COLUMN close_date DATE;

ALTER TABLE sales_pipeline
ALTER COLUMN close_value FLOAT;

SELECT COUNT(close_date) FROM sales_pipeline; 

SELECT * FROM sales_pipeline
WHERE close_date IS NULL
AND close_value IS NULL;


-----sales_teams-----

SELECT * FROM sales_teams ;
SELECT 
  COLUMN_NAME, 
  DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'sales_teams';

---no changes required for sales_teams table----

---Predicting High-Value Deals Using Revenue Percentile Analysis 
-----classifies deals into high, mid, and low-value segments and identifies which sales agents close the most high-value deals.
----High (having close_value above 10,000)
----Mid (having close_value between 1,000 - 10,000)
----Low (having close_value less than 1,000)

WITH sales_segments AS 
                       (SELECT sales_agent,
					   product, 
					   close_value, CASE WHEN
					                close_value >10000 THEN 'High'
		                            WHEN 
									close_value >1000 and close_value < 10000 THEN 'Mid'
		               ELSE 'Low' 
	                   END AS segments
 FROM sales_pipeline)

SELECT TOP 3 sales_agent, 
       COUNT(*) AS 'High_value_sales' 
FROM 
      sales_segments
WHERE 
      close_value > 10000
GROUP BY 
      sales_agent
ORDER BY
      High_value_sales DESC ;


---Sales Pipeline Velocity Analysis
---identify which accounts are moving faster or slower through different deal stages and can highlight bottlenecks.

SELECT account,deal_stage,
	   SUM(close_value)'Total_close_value',
	   SUM(CASE WHEN close_date IS NOT NULL THEN 1 ELSE 0 END) AS closed_deals,
	   AVG(CASE WHEN close_date IS NOT NULL then DATEDIFF(DAY,engage_date,close_date) ELSE NULL END) AS 'Avg_Days_to_close' ,
	   SUM(CASE WHEN close_date IS NULL THEN 1 ELSE 0 END) AS open_deals,
       AVG(CASE WHEN close_date IS NULL 
             THEN DATEDIFF(DAY, engage_date, GETDATE())
             ELSE NULL END) AS 'avg_days_in_pipeline'
FROM
      sales_pipeline 
GROUP BY 
      account, deal_stage
ORDER BY
      Avg_Days_to_close desc ; 

---The above query returns on an average how many days a account takes to close a deal with 'WON' or 'Lost' deal_stage


SELECT 
    account,
    deal_stage,
    COUNT(*) AS total_deals,
    AVG(DATEDIFF(DAY, engage_date, ISNULL(close_date, GETDATE()))) AS avg_days_in_stage
FROM 
    sales_pipeline
where 
    deal_stage in ('Prospecting', 'Engaging')
GROUP BY 
    account, deal_stage
ORDER BY 
    avg_days_in_stage DESC ;



------Average Deal Closing Time by Industry
-----Calculate the average number of days taken to close a deal for each industry

SELECT a.sector ,AVG(DATEDIFF(DAY, engage_date,close_date)) AS avg_days_to_close FROM sales_pipeline sp
JOIN accounts a ON sp.account = a.account
GROUP BY a.sector
ORDER BY avg_days_to_close ;


----Churn Risk Analysis Based on Deal Loss Patterns
----This query identifies accounts with a high risk of churn by calculating the lost deal percentage and the time gap since their last won deal.

SELECT 
    account,
    COUNT(*) AS total_deals,
    SUM(CASE WHEN deal_stage = 'Lost' THEN 1 ELSE 0 END) AS lost_deals,
    CONCAT(CAST(SUM(CASE WHEN deal_stage = 'Lost' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)), '%') AS lost_deal_percentage,
    MAX(CASE WHEN deal_stage = 'Won' THEN close_date ELSE NULL END) AS last_won_date,
    DATEDIFF(DAY, MAX(CASE WHEN deal_stage = 'Won' THEN close_date ELSE NULL END), GETDATE()) AS days_since_last_won
FROM 
    sales_pipeline
GROUP BY 
    account
ORDER BY 
    lost_deal_percentage DESC, 
    days_since_last_won DESC;



-----Sales Seasonality & Revenue Impact Analysis
-----This query identifies seasonal trends in sales performance by analyzing revenue fluctuations across months and years.

SELECT YEAR(close_date) AS 'Year',
       MONTH(close_date) AS 'Month_num',
	   DATENAME(MONTH,close_date) AS 'Months',
	   SUM(close_value) AS 'Total_sales'
FROM 
      sales_pipeline
WHERE 
      close_date IS NOT NULL
GROUP BY 
       YEAR(close_date),
	   MONTH(close_date) ,
	   DATENAME(MONTH,close_date)
ORDER BY 
       Year, Month_num;




	  
















