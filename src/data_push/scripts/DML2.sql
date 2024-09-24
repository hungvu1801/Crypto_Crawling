use cryptocrawling;
select count(*)
from traders t
join exchanges e 
	on e.id = t.crypto_exchange_id
where 1
	and t.id >= (
		select id from traders
        where user_id_foreign = 'b9b74e7188b03d51ac95'
        )
	and e.exchange_name = "Bitget";

select count(*) 
from traders 
where crypto_exchange_id = 2;

select count(open_level), open_level
from position_history_Bitget
where 1
group by open_level
;
-- and order_no = '1215987672306798593'
-- and teacher_id = 'bbb44c7788b33150ad93';
select * from position_history_okx limit 10;

select count(*)
from position_history_Binance
where 1 
-- and status = 'Partially Closed'
-- and teacher_id = '3642466059000851201'
-- order by id desc
-- limit 1000
; -- 2238217

select COUNT(*)
from position_history_Binance
where 1 
-- and teacher_id = '4014471926575328768'
; -- 2239180
select count(*)
from position_history_Bitget
where 1
-- and teacher_id = 'beb2497f8ab6385fa192'
-- order by id desc
-- limit 100
;-- 594285

select COUNT(*)
from position_history_OKX
where 1
-- and close_time = ''
-- and teacher_id = '2CFBF69C2CFB5230'
-- order by id desc
-- limit 5000nngg
; -- 4240690
select count(distinct teacher_id)
from position_history_OKX
where 1 
; -- 12439

select count(id)
from position_history_Bybit
where 1
-- and teacher_id = '+55hUOxDiG4bPuSKOO8EVA=='
-- and status = 'Opened'
-- group by teacher_id, order_no
-- limit 1000
; -- 24564148
select count(distinct teacher_id)
from position_history_Bybit
where 1 
; -- 5164
select count(*) from transactions where transact_date = '2024-09-08';

SELECT *
FROM traders t
    JOIN exchanges e ON e.id = t.crypto_exchange_id
WHERE e.exchange_name = "Bybit"
;
SELECT t.*, e.exchange_name 
FROM traders t
    JOIN exchanges e ON e.id = t.crypto_exchange_id
WHERE e.exchange_name = 'Bybit'
ORDER BY id;

select count(*)
from traders
where crypto_exchange_id = 3;

-- drop table position_history_Bybit;


select *
from position_history_Bybit
where 1 
and pnl = '0'
-- group by teacher_id, order_no
;
SELECT teacher_id, order_no, open_time
FROM position_history_Binance
WHERE teacher_id = '123'
    AND status = 'Partially Closed';

SELECT distinct symbol
		FROM position_history_Binance
		WHERE 
		    id BETWEEN (
		        (SELECT coalesce(max(id), 0) FROM index_history_binance) + 1) 
		            AND 
		        (SELECT MAX(id) FROM position_history_Binance);
select distinct margin_mode
from position_history_Bitget; 

SELECT MAX(id) id, now() updatedate FROM position_history_Bitget;
SELECT MAX(id)
FROM position_history_Binance;
SELECT COALESCE(MAX(id), 0)
FROM index_history_binance;