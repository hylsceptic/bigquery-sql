-- create table `chaingraph-318604.btc_dm.btc_stat_daily` 
--     partition by RANGE_BUCKET(addr_pt, GENERATE_ARRAY(0, 3999, 1))
--     cluster by address
--     as
insert into `chaingraph-318604.btc_dm.btc_stat_daily` 
with 
daily_tx as (
    select 
        *
    from 
        `chaingraph-318604.btc_data.btc_value_flow`
    where true
        and partition_date = DATE_SUB(@run_date, INTERVAL 1 DAY)
        -- and partition_date = '2021-07-28'
)
,
first_last_in_tx as (
    select distinct 
        address, 
        date(block_timestamp) as block_date,
        first_value(`hash`)             over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_in_tx_hash,
        first_value(block_timestamp)    over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_in_tx_block_timestamp,
        first_value(block_number)       over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_in_tx_block_num,
        first_value(net_value)          over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_in_tx_value,
        first_value(net_usd_value)      over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_in_tx_usd_value,
        first_value(`hash`)             over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_in_tx_hash,
        first_value(block_timestamp)    over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_in_tx_block_timestamp,
        first_value(block_number)       over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_in_tx_block_num,
        first_value(net_value)          over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_in_tx_value,
        first_value(net_usd_value)      over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_in_tx_usd_value,
    from 
        daily_tx
    where true
        and net_value > 0
)
,
first_last_out_tx as (
    select distinct 
        address,
        date(block_timestamp) as block_date,
        first_value(`hash`)             over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_out_tx_hash,
        first_value(block_timestamp)    over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_out_tx_block_timestamp,
        first_value(block_number)       over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_out_tx_block_num,
        first_value(net_value)          over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_out_tx_value,
        first_value(net_usd_value)      over (partition by address, date(block_timestamp) order by block_timestamp asc) as first_out_tx_usd_value,
        first_value(`hash`)             over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_out_tx_hash,
        first_value(block_timestamp)    over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_out_tx_block_timestamp,
        first_value(block_number)       over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_out_tx_block_num,
        first_value(net_value)          over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_out_tx_value,
        first_value(net_usd_value)      over (partition by address, date(block_timestamp) order by block_timestamp desc) as last_out_tx_usd_value,
    from 
        daily_tx
    where true
        and net_value < 0
),
first_last_tx as (
    select  
        * 
    from
    (
        first_last_in_tx
        full outer join 
        first_last_out_tx 
        using (address, block_date)    
    )
),
stat1 as (
    select
        address,
        date(block_timestamp) as block_date,
        sum(case when net_value > 0 then 1 else 0 end) as in_tx_cnt,
        sum(case when net_value > 0 then net_value else 0 end) as in_tx_value,
        sum(case when net_value > 0 then net_usd_value else 0 end) as in_tx_value_usd,
        min(case when net_value > 0 then net_value end) as smallest_inflow,
        min(case when net_value > 0 then net_usd_value end) as smallest_inflow_usd,
        max(case when net_value > 0 then net_value end) as largest_inflow,
        max(case when net_value > 0 then net_usd_value end) as largest_inflow_usd,
        sum(case when net_value < 0 then 1 else 0 end) as out_tx_cnt,
        sum(case when net_value < 0 then -net_value else 0 end) as out_tx_value,
        sum(case when net_value < 0 then -net_usd_value else 0 end) as out_tx_value_usd,
        min(case when net_value < 0 then -net_value end) as smallest_outflow,
        min(case when net_value < 0 then -net_usd_value end) as smallest_outflow_usd,
        max(case when net_value < 0 then -net_value end) as largest_outflow,
        max(case when net_value < 0 then -net_usd_value end) as largest_outflow_usd,
    from 
        daily_tx 
    group by address, block_date 
)
select 
    mod(cast(concat('0x', substr(cast(md5(b.address) as string format 'hex'), -3)) as int64), 4000) as addr_pt,
    address, 
    block_date,
    in_tx_cnt,
    in_tx_value,
    case when smallest_inflow is not null then smallest_inflow else 0 end as smallest_inflow,
    case when largest_inflow is not null then largest_inflow else 0 end as largest_inflow,
    out_tx_cnt,
    out_tx_value,
    case when smallest_outflow is not null then smallest_outflow else 0 end as smallest_outflow,
    case when largest_outflow is not null then largest_outflow else 0 end as largest_outflow,
    in_tx_cnt + out_tx_cnt as tx_cnt,
    in_tx_value + out_tx_value as turnover,
    in_tx_value_usd + out_tx_value_usd as turnover_usd,
    in_tx_value_usd,
    case when smallest_inflow_usd is not null then smallest_inflow_usd else 0 end as smallest_inflow_usd,
    case when largest_inflow_usd is not null then largest_inflow_usd else 0 end as largest_inflow_usd,
    out_tx_value_usd,
    case when smallest_outflow_usd is not null then smallest_outflow_usd else 0 end as smallest_outflow_usd,
    case when largest_outflow_usd is not null then largest_outflow_usd else 0 end as largest_outflow_usd,
    b.* except (address, block_date),
    'BTC' as symbol
from
    (
        stat1 a 
        join
        first_last_tx b
        using (address, block_date)
    )
;