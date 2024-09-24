use cryptocrawling;

select count(*)
from staging_crawling_item
where 1
-- and crypto_exchange = 'Binance'
-- and trader_name like '稳中求胜丶'
-- and user_id is null
;

SELECT count(*) 
FROM (
	SELECT *, ROW_NUMBER() OVER 
	(PARTITION BY user_id, crypto_exchange ORDER BY crypto_exchange) 
	AS number
	FROM staging_crawling_item) A
WHERE A.number = 1
;

select * from index_update;

select count(*) from traders;
show tables;
-- SET SQL_SAFE_UPDATES = 0;
-- delete from staging_crawling_item;
-- SET SQL_SAFE_UPDATES = 1;

with tb1 as
	(select 
		a.trader_id, b.crypto_exchange_id, a.transact_date, b.trader_name, a.roi, e.exchange_name
		, row_number() over (partition by a.transact_date , b.crypto_exchange_id order by a.roi desc) as rn 
	from transactions a
	join traders b on a.trader_id = b.id
    join exchanges e on e.id = b.crypto_exchange_id)
select count(tb1.trader_id), tb1.trader_id, tb1.exchange_name
from tb1 
where rn >= 1 and rn <= 5
group by tb1.trader_id, tb1.exchange_name
having count(tb1.trader_id) > 1
;
WITH tb1 AS (
    SELECT 
        a.transact_date, 
        b.trader_name, 
        a.roi, 
        e.exchange_name,
        ROW_NUMBER() OVER (PARTITION BY a.transact_date, b.crypto_exchange_id ORDER BY a.roi DESC) AS rn
    FROM 
        transactions a
    JOIN 
        traders b ON a.trader_id = b.id
    JOIN 
        exchanges e ON e.id = b.crypto_exchange_id
    WHERE 
        e.exchange_name = 'Binance'
)
SELECT 
    a.rn AS 'Rank',
    MAX(CASE WHEN a.transact_date = '2024-08-09' THEN a.trader_name END) AS '08-09',
    MAX(CASE WHEN a.transact_date = '2024-08-12' THEN a.trader_name END) AS '08-12',
    MAX(CASE WHEN a.transact_date = '2024-08-13' THEN a.trader_name END) AS '08-13',
    MAX(CASE WHEN a.transact_date = '2024-08-14' THEN a.trader_name END) AS '08-14',
    MAX(CASE WHEN a.transact_date = '2024-08-15' THEN a.trader_name END) AS '08-15',
    MAX(CASE WHEN a.transact_date = '2024-08-16' THEN a.trader_name END) AS '08-16',
    MAX(CASE WHEN a.transact_date = '2024-08-17' THEN a.trader_name END) AS '08-17',
    MAX(CASE WHEN a.transact_date = '2024-08-18' THEN a.trader_name END) AS '08-18',
    MAX(CASE WHEN a.transact_date = '2024-08-19' THEN a.trader_name END) AS '08-19',
    MAX(CASE WHEN a.transact_date = '2024-08-20' THEN a.trader_name END) AS '08-20',
    MAX(CASE WHEN a.transact_date = '2024-08-21' THEN a.trader_name END) AS '08-21',
    MAX(CASE WHEN a.transact_date = '2024-08-22' THEN a.trader_name END) AS '08-22',
    MAX(CASE WHEN a.transact_date = '2024-08-23' THEN a.trader_name END) AS '08-23',
    MAX(CASE WHEN a.transact_date = '2024-08-24' THEN a.trader_name END) AS '08-24',
    MAX(CASE WHEN a.transact_date = '2024-08-25' THEN a.trader_name END) AS '08-25'
FROM 
    tb1 a
WHERE 
    a.transact_date IN 
    ('2024-08-09', '2024-08-12', '2024-08-13', 
    '2024-08-14', '2024-08-15', '2024-08-16', 
    '2024-08-17', '2024-08-18','2024-08-19', 
    '2024-08-20', '2024-08-21','2024-08-22', 
    '2024-08-23', '2024-08-24', '2024-08-25', 
    '2024-08-26')
    AND a.rn BETWEEN 1 AND 5
GROUP BY 
    a.rn
ORDER BY 
    a.rn;
        
select * from traders 
where 1
and url = '+73AiQuLbjckJMfZHzX+LA=='
-- and id = 13177
;

select * from exchanges;
select count(*) 
from transactions 
where transact_date = curdate()
;
SELECT COALESCE(MAX(id), 0)
FROM position_history_Bybit
;

SELECT t.*, e.exchange_name 
FROM traders t
    JOIN exchanges e ON e.id = t.crypto_exchange_id
WHERE e.exchange_name = 'Binance'
ORDER BY id;
