insert_binance__tbl_detail = """
-- Binance
insert into tbl_detail_all(
	order_no
	, open_time
	, close_time
	, open_price
	, close_price
	, pnl
	, token
	, transact_ccy
	, position
	, margin_mode
	, trader_id
	, updated_at
)
with tbl_lookup as 
	(
		select t.id, t.url, t.user_id_foreign, e.exchange_name  
		from traders t
		join exchanges e
		on e.id = t.crypto_exchange_id
		where 1
		and exchange_name = 'Binance'
		order by t.id
	), 
	tbl_push as (
		SELECT trim(teacher_id) teacher_id, trim(order_no) order_no, trim(open_time) open_time, trim(close_time) close_time,
				trim(avg_cost) avg_cost, trim(avg_close_price) avg_close_price, trim(closing_pnl) closing_pnl,
				trim(symbol) symbol, trim(side) side, trim(isolated) isolated
		FROM position_history_Binance
		WHERE 
		    id BETWEEN (
		        (SELECT coalesce(max(id), 0) FROM index_history_binance) + 1) 
		            AND 
		        (SELECT MAX(id) FROM position_history_Binance))
select 
	cast(order_no as char(50)) as order_no,
	FROM_UNIXTIME(CAST(open_time AS UNSIGNED)/1000) AS open_time,
    FROM_UNIXTIME(CAST(CASE WHEN close_time = '' THEN NULL ELSE close_time END AS UNSIGNED)/1000) AS close_time,
    CAST(avg_cost AS DECIMAL(30, 15)) as open_price,
    CAST(avg_close_price AS DECIMAL(30, 15)) as close_price,
	CAST(closing_pnl AS DECIMAL(30, 15)) as pnl,
    SUBSTRING_INDEX(symbol, 'USDT', 1) AS token,
    'USDT' AS transact_ccy,
    side as position,
    isolated as margin_mode,
    b.id as trader_id,
    now() as updated_at
from tbl_push a
join tbl_lookup b on a.teacher_id = b.user_id_foreign

;
"""
insert_okx__tbl_detail = """
insert into tbl_detail_all(
	order_no
	, open_time
	, close_time
	, open_price
	, close_price
	, pnl, token
	, transact_ccy
	, position
	, margin_mode
	, trader_id
	, updated_at
)
with tbl_lookup as 
	(
		select t.id, t.url, t.user_id_foreign, e.exchange_name  
		from traders t
		join exchanges e
		on e.id = t.crypto_exchange_id
		where 1
		and exchange_name = 'OKX'
		order by t.id
	), 
	tbl_push as (
		SELECT trim(teacher_id) teacher_id, trim(order_no) order_no, trim(open_time) open_time, trim(close_time) close_time,
				trim(open_avg_px) open_avg_px, trim(close_avg_px) close_avg_px, trim(close_pnl) close_pnl,
				trim(symbol) symbol, trim(ccy) ccy, trim(position) position, trim(margin_mode) margin_mode
		FROM position_history_OKX
		WHERE 
		    id BETWEEN (
		        (SELECT coalesce(max(id), 0) FROM index_history_okx) + 1) 
		            AND 
		        (SELECT MAX(id) FROM position_history_OKX))
select 
	cast(order_no as char(50)) as order_no
	,FROM_UNIXTIME(CAST(open_time AS UNSIGNED)/1000) AS open_time
	,FROM_UNIXTIME(CAST(CASE WHEN close_time = '' THEN NULL ELSE close_time END AS UNSIGNED)/1000) AS close_time
    ,CAST(open_avg_px AS DECIMAL(30, 15)) as open_price
    ,CAST(close_avg_px AS DECIMAL(30, 15)) as close_price
    ,CAST(close_pnl AS DECIMAL(30, 15)) as pnl
    ,SUBSTRING_INDEX(symbol, '-USDT', 1) AS token
    ,ccy AS transact_ccy
    ,CONCAT(UCASE(LEFT(position, 1)),
              LCASE(SUBSTRING(position, 2))) as position
    ,CONCAT(UCASE(LEFT(margin_mode, 1)), 
              LCASE(SUBSTRING(margin_mode, 2))) as margin_mode
    ,b.id as trader_id
    ,now() as updated_at
from tbl_push a
join tbl_lookup b on a.teacher_id = b.user_id_foreign
;
"""

insert_bybit__tbl_detail = """
insert into tbl_detail_all(
	order_no
	, open_time
	, close_time
	, open_price
	, close_price
	, pnl, token
	, transact_ccy
	, position
	, margin_mode
	, trader_id
	, updated_at
)
with tbl_lookup as 
	(
		select t.id, t.url, t.user_id_foreign, e.exchange_name  
		from traders t
		join exchanges e
		on e.id = t.crypto_exchange_id
		where 1
		and exchange_name = 'Bybit'
		order by t.id
	), 
	tbl_push as (
		SELECT trim(teacher_id) teacher_id, trim(order_no) order_no, trim(open_time) open_time, trim(close_time) close_time,
				trim(entry_price) entry_price, trim(closed_price) closed_price, trim(order_net_profit) order_net_profit,
				trim(symbol) symbol, trim(side) side, trim(is_isolated) is_isolated
		FROM position_history_Bybit
		WHERE 
		    id BETWEEN (
		        (SELECT coalesce(max(id), 0) FROM index_history_bybit) + 1) 
		            AND 
		        (SELECT MAX(id) FROM position_history_Bybit))
select
	order_no,
	FROM_UNIXTIME(CAST(open_time AS UNSIGNED)/1000) AS open_time,
    FROM_UNIXTIME(CAST(CASE WHEN close_time = '' THEN NULL ELSE close_time END AS UNSIGNED)/1000) AS close_time,
    CAST(entry_price AS DECIMAL(30, 15)) as open_price,
    CAST(closed_price AS DECIMAL(30, 15)) as close_price,
    ROUND(CAST(order_net_profit AS DECIMAL(30, 15)) / 100000000, 10) as pnl,
    SUBSTRING_INDEX(symbol, 'USDT', 1) AS token,
	'USDT' AS transact_ccy,
	case 
		when side = 'Buy' then 'Long'
		else 'Short'
	end as position,
	case 
		when is_isolated = 1 then 'Isolated'
		else 'Cross'
	end as margin_mode,
    b.id as trader_id,
    now() as updated_at
from tbl_push a
join tbl_lookup b on a.teacher_id = b.url 
;
"""

insert_bitget__tbl_detail = """
insert into tbl_detail_all(
	order_no
	, open_time
	, close_time
	, open_price
	, close_price
	, pnl, token
	, transact_ccy
	, position
	, margin_mode
	, trader_id
	, updated_at
)
with tbl_lookup as 
	(
		select t.id, t.url, t.user_id_foreign, e.exchange_name  
		from traders t
		join exchanges e
		on e.id = t.crypto_exchange_id
		where 1
		and exchange_name = 'Bitget'
		order by t.id
	), 
	tbl_push as (
		SELECT
			trim(teacher_id) teacher_id, trim(order_no) order_no, trim(open_time) open_time, trim(margin_mode) margin_mode,
			trim(close_time) close_time, trim(open_avg_price) open_avg_price, trim(close_avg_price) close_avg_price, 
			trim(net_profit) net_profit, trim(left_symbol) left_symbol, trim(token_id) token_id, trim(position) position
		FROM position_history_Bitget
		WHERE 
		    id BETWEEN (
		        (SELECT coalesce(max(id), 0) FROM index_history_bitget) + 1) 
		            AND 
		        (SELECT MAX(id) FROM position_history_Bitget))
select
	order_no,
	FROM_UNIXTIME(CAST(open_time AS UNSIGNED)/1000) AS open_time,
    FROM_UNIXTIME(CAST(CASE WHEN close_time = '' THEN NULL ELSE close_time END AS UNSIGNED)/1000) AS close_time,
    CAST(open_avg_price AS DECIMAL(30, 15)) as open_price,
    CAST(close_avg_price AS DECIMAL(30, 15)) as close_price,
	CAST(net_profit AS DECIMAL(30,15)) as pnl,
    left_symbol AS token,
	token_id AS transact_ccy,
	case 
		when position = '1' then 'Long'
		else 'Short'
	end as position,
	case 
		when margin_mode = 2 then 'Cross'
		else 'Isolated'
	end as margin_mode,
    b.id as trader_id,
    now() as updated_at
from tbl_push a
join tbl_lookup b on a.teacher_id = b.user_id_foreign 
;
"""