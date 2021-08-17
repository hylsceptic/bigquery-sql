-- create or replace table `chaingraph-318604.eth_dm.eth_token_stat_daily` as
-- create or replace table `chaingraph-318604.eth_dm.eth_token_stat_daily` 
--     partition by RANGE_BUCKET(addr_pt, GENERATE_ARRAY(0, 3999, 1))
--     cluster by address
--     as
-- delete from `chaingraph-318604.eth_dm.eth_token_stat_daily`  where 1=1
-- truncate table `chaingraph-318604.eth_dm.eth_token_stat_daily`;
insert into `chaingraph-318604.eth_dm.eth_token_stat_daily`
with
tx_subset as (
    select 
        `hash`,
        from_address, 
        to_address,
        block_timestamp,
        input,
        receipt_status
    from
        `bigquery-public-data.crypto_ethereum.transactions`
    where true
        -- and DATE(block_timestamp) <= '2021-08-11'
        and date(block_timestamp) = date_sub(@run_date, interval 1 day)
)
,
direct_token_transfers as 
(
    select 
        a.*
    from 
        (
            SELECT 
                transaction_hash,
                from_address,
                to_address,
                token_address,
                safe_cast(value as numeric) as value,
                block_timestamp,
                block_number,
            FROM 
                `bigquery-public-data.crypto_ethereum.token_transfers` 
            WHERE true 
                -- and DATE(block_timestamp) <= '2021-08-11'
                and date(block_timestamp) = date_sub(@run_date, interval 1 day)
        ) a 
        inner join 
        (
            select 
                `hash`
            from 
                tx_subset
            WHERE starts_with(input, '0xa9059cbb')
                
        ) b
        on a.`transaction_hash` = b.`hash`
)
,
eth_token_price as (
    select 
        address,
        price_day,
        price,
        symbol
    from
        `chaingraph-318604.ethereum_data.eth_token_price`
    where true
        -- and price_day <= '2021-08-11'
        and price_day = date_sub(@run_date, interval 1 day)
)
,
eth_transfers as 
(
    SELECT
        transaction_hash,
        from_address,
        to_address,
        '0x0000000000000000000000000000000000000000' as token_address,
        block_timestamp,
        block_number,
        value * pow(10, -18)  as value, 
        'eth' as symbol
    FROM 
        `bigquery-public-data.crypto_ethereum.traces` 
    WHERE true
        -- and DATE(block_timestamp) <= '2021-08-11'
        and DATE(block_timestamp) = date_sub(@run_date, interval 1 day)
        and value > 0
    --     and from_address is not null -- miner rewards
        and to_address is not null -- smart contract creation but failed.
)
,
suc_txhash as (
    (
        select
            `hash` as transaction_hash
        from 
            tx_subset
        where true 
            and receipt_status = 1
    )
    union distinct 
    (
        select 
            transaction_hash
        from
            `chaingraph-318604.ethereum_metrics.tx_before_byzantium`
        where receipt_status = 1
    )
)
,
eth_suc_transfers as (
    select
        a.*
    from
    (
        eth_transfers a
        join
        suc_txhash b
        using (transaction_hash)
    )
)
,
valid_tokens as (
    select 
        a.address as token_address,
        a.symbol,
        b.decimals
    from
        (select distinct address, symbol from `chaingraph-318604.ethereum_data.eth_token_price`) a
        left join 
        (
            select * from
            (
                select address, decimals from `bigquery-public-data.crypto_ethereum.amended_tokens` where decimals is not null
            )
            union all 
            (
                select * from `chaingraph-318604.ethereum_data.token_decimals`
            )
        ) b using(address) 
    where b.decimals is not null
)
,
direct_valid_token_transfers as (
    select 
        a.* except (value),
        a.value * pow(10, -cast(b.decimals as int64)) as value,
        b.symbol
    from
    (
        direct_token_transfers a
        join
        valid_tokens b
        using (token_address)
    )
)
,
eth_token_transfers as (
    select * from eth_suc_transfers 
    union all
    select * from direct_valid_token_transfers
)
,
eth_token_transfers_price as (
    select 
        a.*,
        a.value * coalesce(b.price, 0) as usd_value,
    from
        eth_token_transfers a
        join
        eth_token_price b
        on (date(a.block_timestamp) = b.price_day and a.token_address = b.address)
)
,
in_tx_addr as (
    select 
        to_address as address,
        date(block_timestamp) as block_date,
        token_address,
        any_value(symbol) as symbol,
        count(1) as in_tx_cnt,
        sum(value) as in_tx_value,
        sum(usd_value) as in_tx_value_usd,
        min(value) as smallest_inflow,
        min(usd_value) as smallest_inflow_usd,
        max(value) as largest_inflow,
        max(usd_value) as largest_inflow_usd,
    from
        eth_token_transfers_price
    where true
        and to_address is not null
    group by address, date(block_timestamp), token_address
)
,
out_tx_addr as (
    select 
        from_address as address,
        date(block_timestamp) as block_date,
        token_address,
        any_value(symbol) as symbol,
        count(1) as out_tx_cnt,
        sum(value) as out_tx_value,
        sum(usd_value) as out_tx_value_usd,
        min(value) as smallest_outflow,
        min(usd_value) as smallest_outflow_usd,
        max(value) as largest_outflow,
        max(usd_value) as largest_outflow_usd,
    from
        eth_token_transfers_price
    where true
        and from_address is not null
    group by address, date(block_timestamp), token_address
)
,
stat1 as (
    select 
        address,
        block_date,
        token_address,
        symbol,
        coalesce(in_tx_cnt, 0) as in_tx_cnt,
        coalesce(in_tx_value, 0) as in_tx_value,
        coalesce(in_tx_value_usd, 0) as in_tx_value_usd,
        coalesce(smallest_inflow, 0) as smallest_inflow,
        coalesce(smallest_inflow_usd, 0) as smallest_inflow_usd,
        coalesce(largest_inflow, 0) as largest_inflow,
        coalesce(largest_inflow_usd, 0) as largest_inflow_usd,
        coalesce(out_tx_cnt, 0) as out_tx_cnt,
        coalesce(out_tx_value, 0) as out_tx_value,
        coalesce(out_tx_value_usd, 0) as out_tx_value_usd,
        coalesce(smallest_outflow, 0) as smallest_outflow,
        coalesce(smallest_outflow_usd, 0) as smallest_outflow_usd,
        coalesce(largest_outflow, 0) as largest_outflow,
        coalesce(largest_outflow_usd, 0) as largest_outflow_usd,
        coalesce(in_tx_value, 0) + coalesce(out_tx_cnt, 0) as turnover,
        coalesce(in_tx_value_usd, 0) + coalesce(out_tx_value_usd, 0) as turnover_usd,
    from 
    (
        in_tx_addr a
        full outer join 
        out_tx_addr b
        using (address, block_date, token_address, symbol)
    )
)
,
first_last_in_tx as (
    select distinct 
        to_address as address,
        date(block_timestamp) as block_date,
        token_address,
        first_value(transaction_hash)   over (partition by to_address, date(block_timestamp), token_address order by block_timestamp asc) as first_in_tx_hash,
        first_value(block_timestamp)    over (partition by to_address, date(block_timestamp), token_address order by block_timestamp asc) as first_in_tx_block_timestamp,
        first_value(block_number)       over (partition by to_address, date(block_timestamp), token_address order by block_timestamp asc) as first_in_tx_block_num,
        first_value(value)              over (partition by to_address, date(block_timestamp), token_address order by block_timestamp asc) as first_in_tx_value,
        first_value(usd_value)          over (partition by to_address, date(block_timestamp), token_address order by block_timestamp asc) as first_in_tx_usd_value,
        first_value(transaction_hash)   over (partition by to_address, date(block_timestamp), token_address order by block_timestamp desc) as last_in_tx_hash,
        first_value(block_timestamp)    over (partition by to_address, date(block_timestamp), token_address order by block_timestamp desc) as last_in_tx_block_timestamp,
        first_value(block_number)       over (partition by to_address, date(block_timestamp), token_address order by block_timestamp desc) as last_in_tx_block_num,
        first_value(value)              over (partition by to_address, date(block_timestamp), token_address order by block_timestamp desc) as last_in_tx_value,
        first_value(usd_value)          over (partition by to_address, date(block_timestamp), token_address order by block_timestamp desc) as last_in_tx_usd_value,
    from 
        eth_token_transfers_price
    where true
        and to_address is not null
)
,
first_last_out_tx as (
    select distinct 
        from_address as address,
        date(block_timestamp) as block_date,
        token_address,
        first_value(transaction_hash)   over (partition by from_address, date(block_timestamp), token_address order by block_timestamp asc) as first_out_tx_hash,
        first_value(block_timestamp)    over (partition by from_address, date(block_timestamp), token_address order by block_timestamp asc) as first_out_tx_block_timestamp,
        first_value(block_number)       over (partition by from_address, date(block_timestamp), token_address order by block_timestamp asc) as first_out_tx_block_num,
        first_value(value)              over (partition by from_address, date(block_timestamp), token_address order by block_timestamp asc) as first_out_tx_value,
        first_value(usd_value)          over (partition by from_address, date(block_timestamp), token_address order by block_timestamp asc) as first_out_tx_usd_value,
        first_value(transaction_hash)   over (partition by from_address, date(block_timestamp), token_address order by block_timestamp desc) as last_out_tx_hash,
        first_value(block_timestamp)    over (partition by from_address, date(block_timestamp), token_address order by block_timestamp desc) as last_out_tx_block_timestamp,
        first_value(block_number)       over (partition by from_address, date(block_timestamp), token_address order by block_timestamp desc) as last_out_tx_block_num,
        first_value(value)              over (partition by from_address, date(block_timestamp), token_address order by block_timestamp desc) as last_out_tx_value,
        first_value(usd_value)          over (partition by from_address, date(block_timestamp), token_address order by block_timestamp desc) as last_out_tx_usd_value,
    from 
        eth_token_transfers_price
    where true
        and from_address is not null
)
,
first_last_tx as (
    select  
        *
    from
    (
        first_last_in_tx a 
        full outer join 
        first_last_out_tx b 
        using (address, block_date, token_address)
    )
)
,
stat2 as (
    select * from
    (
        stat1 a 
        full outer join
        first_last_tx b
        using (address, block_date, token_address)
    )
)
,
addr_tx_cnt as (
    select 
        address,
        date(block_timestamp) as block_date,
        count(1) as tx_cnt
    from
        (
            select from_address as address, block_timestamp from tx_subset
            union all
            select to_address as address, block_timestamp from tx_subset
        )
    where address is not null
    group by address, date(block_timestamp)
)
,
stat as (
    select 
        mod(cast(concat('0x', substr(address, -3)) as int64), 4000) as addr_pt,
        a.*,
        coalesce(b.tx_cnt, 0) as tx_cnt
    from 
        (
            stat2 a
            full outer join 
            addr_tx_cnt b
            using (address, block_date)
        )
)
select
    *
from 
    stat
;