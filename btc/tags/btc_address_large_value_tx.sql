-- create or replace table `chaingraph-318604.btc_tags.btc_address_large_value_tx` as 
with 
value_flow as (
    select 
        address,
        in_usd_value,
        out_usd_value
    from
        `chaingraph-318604.btc_data.btc_value_flow`
    where true
        -- and partition_date = '2021-08-01'
        -- and partition_date <= '2021-08-02'
        -- and block_timestamp_month = DATE_TRUNC(DATE_SUB(@run_date, INTERVAL 1 DAY), MONTH)
        and partition_date = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
addr_in_tx_10k_usd as (
    select distinct 
        address,
        'addr_in_tx_10k_usd' as feature,
        '401' as plots,
        1 as idx
    from
        value_flow
    where true 
        and in_usd_value >= 10000
)
,
addr_in_tx_50k_usd as (
    select distinct 
        address,
        'addr_in_tx_50k_usd' as feature,
        '401' as plots,
        2 as idx
    from
        value_flow
    where true 
        and in_usd_value >= 50000
)
,
addr_in_tx_100k_usd as (
    select distinct 
        address,
        'addr_in_tx_100k_usd' as feature,
        '401' as plots,
        3 as idx
    from
        value_flow
    where true 
        and in_usd_value >= 100000
)
,
addr_out_tx_10k_usd as (
    select distinct 
        address,
        'addr_out_tx_10k_usd' as feature,
        '401' as plots,
        4 as idx
    from
        value_flow
    where true 
        and out_usd_value >= 10000
)
,
addr_out_tx_50k_usd as (
    select distinct 
        address,
        'addr_out_tx_50k_usd' as feature,
        '401' as plots,
        5 as idx
    from
        value_flow
    where true 
        and out_usd_value >= 50000
)
,
addr_out_tx_100k_usd as (
    select distinct 
        address,
        'addr_out_tx_100k_usd' as feature,
        '401' as plots,
        6 as idx
    from
        value_flow
    where true 
        and out_usd_value >= 100000
)
,
res as (
    select * from
    (
        select * from addr_in_tx_10k_usd
        union distinct 
        select * from addr_in_tx_50k_usd
        union distinct 
        select * from addr_in_tx_100k_usd
        union distinct 
        select * from addr_out_tx_10k_usd
        union distinct 
        select * from addr_out_tx_50k_usd
        union distinct 
        select * from addr_out_tx_100k_usd       
    )
)
-- select * from res
select 
    a.address as address,
    a.feature as feature,
    a.plots as plots,
    a.idx as idx
from
(
    res a
    left join 
    (
        select 
            * 
        from 
            `chaingraph-318604.btc_tags.btc_address_large_value_tx`
    ) b
    on a.address = b.address
)
where b.feature is null
;
