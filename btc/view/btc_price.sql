select * from 
    (
        SELECT
            openTimeStamp as price_timestamp, 
            max(cast(close as numeric)) as price
            FROM `chaingraph-318604.BINANCE_T_DATASET.Binance_Klines_1d` where symbol = "BTCUSDT" group by openTimeStamp
     )
    union all select *
    from  `chaingraph-318604.BINANCE_T_DATASET.btc_price`