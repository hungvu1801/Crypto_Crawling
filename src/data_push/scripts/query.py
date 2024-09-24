select_staging_get_latest_row = 'SELECT * FROM staging_crawling_item ORDER BY id DESC LIMIT 1;'

select_staging_get_test_row = 'SELECT * FROM staging_crawling_item LIMIT 1;'

select_unloaded_data_from_staging_tbl = """
SELECT *
FROM staging_crawling_item
WHERE id BETWEEN (
    (SELECT coalesce(max(id), 0) FROM index_update) + 1) 
        AND 
    (SELECT MAX(id) FROM staging_crawling_item);
"""

select_unloaded_data_from_tbl_history_binance = """
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
		SELECT *
		FROM position_history_Binance
		WHERE 
		    id BETWEEN (
		        (SELECT coalesce(max(id), 0) FROM index_history_binance) + 1) 
		            AND 
		        (SELECT MAX(id) FROM position_history_Binance))
select 	
	order_no,
	FROM_UNIXTIME(CAST(open_time AS UNSIGNED)/1000) AS open_time,
    FROM_UNIXTIME(CAST(close_time AS UNSIGNED)/1000) AS close_time,
    CAST(closing_pnl AS FLOAT) as pnl,
    SUBSTRING_INDEX(symbol, 'USDT', 1) AS token,
    'USDT' AS transact_ccy,
    side,
    isolated as pos_type,
    b.id as trader_id,
    now() as updated_at
from tbl_push a
join tbl_lookup b on a.teacher_id = b.user_id_foreign ;
"""

select_unloaded_data_from_tbl_history_okx = """
SELECT *
FROM position_history_OKX
WHERE 
    id BETWEEN (
        (SELECT coalesce(max(id), 0) FROM index_history_okx) + 1) 
            AND 
        (SELECT MAX(id) FROM position_history_OKX)
;
"""

select_unloaded_data_from_tbl_history_bybit = """
SELECT *
FROM position_history_Bybit
WHERE 
    id BETWEEN (
        (SELECT coalesce(max(id), 0) FROM index_history_bybit) + 1) 
            AND 
        (SELECT MAX(id) FROM position_history_Bybit)
;
"""

select_unloaded_data_from_tbl_history_bitget = """
SELECT *
FROM position_history_Bitget
WHERE 
    id BETWEEN (
        (SELECT coalesce(max(id), 0) FROM index_history_bitget) + 1) 
            AND 
        (SELECT MAX(id) FROM position_history_Bitget)
;
"""

select_ranked_based_on_exchange = """
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
        e.exchange_name = 'Bybit'
)
SELECT 
    a.rn AS 'Rank'
    , MAX(CASE WHEN a.transact_date = '2024-08-12' THEN a.trader_name END) AS '08-12'
    , MAX(CASE WHEN a.transact_date = '2024-08-09' THEN a.trader_name END) AS '08-09'
    , MAX(CASE WHEN a.transact_date = '2024-08-13' THEN a.trader_name END) AS '08-13'
    , MAX(CASE WHEN a.transact_date = '2024-08-14' THEN a.trader_name END) AS '08-14'
    , MAX(CASE WHEN a.transact_date = '2024-08-15' THEN a.trader_name END) AS '08-15'
    , MAX(CASE WHEN a.transact_date = '2024-08-16' THEN a.trader_name END) AS '08-16'
    , MAX(CASE WHEN a.transact_date = '2024-08-17' THEN a.trader_name END) AS '08-17'
    , MAX(CASE WHEN a.transact_date = '2024-08-18' THEN a.trader_name END) AS '08-18'
    , MAX(CASE WHEN a.transact_date = '2024-08-19' THEN a.trader_name END) AS '08-19'
    , MAX(CASE WHEN a.transact_date = '2024-08-20' THEN a.trader_name END) AS '08-20'
    , MAX(CASE WHEN a.transact_date = '2024-08-21' THEN a.trader_name END) AS '08-21'
    , MAX(CASE WHEN a.transact_date = '2024-08-22' THEN a.trader_name END) AS '08-22'
    , MAX(CASE WHEN a.transact_date = '2024-08-23' THEN a.trader_name END) AS '08-23'
    , MAX(CASE WHEN a.transact_date = '2024-08-24' THEN a.trader_name END) AS '08-24'
    , MAX(CASE WHEN a.transact_date = '2024-08-25' THEN a.trader_name END) AS '08-25'
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
"""

select_traders_filter_exchange_name = """
SELECT t.*, e.exchange_name 
FROM traders t
    JOIN exchanges e ON e.id = t.crypto_exchange_id
WHERE e.exchange_name = :exchange_name
ORDER BY id;
"""

select_all_traders = """
SELECT * FROM traders;
"""

select_transaction_filter_by_order_id_Bitget = """
SELECT id
FROM position_history_Bitget
WHERE order_no = :order_no AND teacher_id = :teacher_id;
"""

select_transaction_filter_by_order_id_Binance = """
SELECT id
FROM position_history_Binance
WHERE order_no = :order_no AND teacher_id = :teacher_id;
"""

select_transaction_filter_by_order_id_Bybit = """
SELECT id
FROM position_history_Bybit
WHERE order_no = :order_no AND teacher_id = :teacher_id;
"""

select_transaction_filter_by_order_id_OKX = """
SELECT id
FROM position_history_OKX
WHERE order_no = :order_no AND teacher_id = :teacher_id;
"""

select_transaction_filter_by_order_id_and_status_Binance = """
SELECT teacher_id, order_no, open_time
FROM position_history_Binance
WHERE teacher_id = :teacher_id
    AND status = 'Partially Closed';
"""

select_transaction_filter_by_order_id_and_status_Bybit = """
SELECT teacher_id, order_no, open_time
FROM position_history_Bybit
WHERE teacher_id = :teacher_id
    AND status = 'Opened';
"""
select_transaction_filter_by_order_id_and_status_okx = """
SELECT teacher_id, order_no, open_time, close_time
FROM position_history_OKX
WHERE teacher_id = :teacher_id
    AND close_time = '';
"""

select_get_status_Binance = """
SELECT status
FROM position_history_Binance
WHERE order_no = :order_no 
AND teacher_id = :teacher_id
;
"""

select_get_status_Bybit = """
SELECT status
FROM position_history_Bybit
WHERE order_no = :order_no 
AND teacher_id = :teacher_id
"""

select_get_status_OKX = """
SELECT close_time
FROM position_history_OKX
WHERE order_no = :order_no 
AND teacher_id = :teacher_id
"""

update_transaction_Binance = """
UPDATE position_history_Binance 
SET 
    close_time = :close_time,
    avg_cost = :avg_cost,
    avg_close_price = :avg_close_price,
    closing_pnl = :closing_pnl,
    max_open_interest = :max_open_interest,
    status = :status
WHERE order_no = :order_no AND teacher_id = :teacher_id;
"""
update_transaction_Binance = update_transaction_Binance.replace('\n',' ')

update_transaction_Bybit = """
UPDATE position_history_Bybit
SET 
    closed_price = :closed_price,
    close_time = :close_time,
    cum_funding_fee = :cum_funding_fee,
    close_cum_exec_fee = :close_cum_exec_fee,
    order_net_profit = :order_net_profit,
    order_net_profit_rate = :order_net_profit_rate,
    pos_closed_time = :pos_closed_time,
    status = :status
WHERE order_no = :order_no AND teacher_id = :teacher_id;
"""
update_transaction_Bybit = update_transaction_Bybit.replace('\n',' ')

update_transaction_OKX = """
UPDATE position_history_OKX
SET 
    close_avg_px = :close_avg_px,
    close_pnl = :close_pnl,
    fee = :fee,
    funding_fee = :funding_fee,
    close_time = :close_time,
    pnl = :pnl,
    pnl_ratio = :pnl_ratio
WHERE order_no = :order_no AND teacher_id = :teacher_id;
"""
update_transaction_OKX = update_transaction_OKX.replace('\n',' ')

select_maxid_index_binance = """
SELECT COALESCE(MAX(id), 0)
FROM index_history_binance;
"""
select_maxid_history_binance = """
SELECT MAX(id)
FROM position_history_Binance;
"""

select_maxid_index_okx = """
SELECT COALESCE(MAX(id), 0)
FROM index_history_okx;
"""
select_maxid_history_okx = """
SELECT MAX(id)
FROM position_history_OKX;
"""

select_maxid_index_bybit = """
SELECT COALESCE(MAX(id), 0)
FROM index_history_bybit;
"""
select_maxid_history_bybit = """
SELECT MAX(id)
FROM position_history_Bybit;
"""

select_maxid_index_bitget = """
SELECT COALESCE(MAX(id), 0)
FROM index_history_bitget;
"""
select_maxid_history_bitget = """
SELECT MAX(id)
FROM position_history_Bitget;
"""