-- create or replace table `chaingraph-318604.ethereum_tags.address_large_value_tx` as
-- create or replace table `chaingraph-318604.eth_dm.eth_token_stat_daily` 
--     partition by RANGE_BUCKET(addr_pt, GENERATE_ARRAY(0, 3999, 1))
--     cluster by address
--     as
-- delete from `chaingraph-318604.eth_dm.eth_token_stat_daily`  where 1=1
-- truncate table `chaingraph-318604.eth_dm.eth_token_stat_daily`;
insert into `chaingraph-318604.ethereum_tags.address_large_value_tx`
with
suc_txhash as (
    (
        select
            `hash`
        from 
            `bigquery-public-data.crypto_ethereum.transactions` 
        where true 
            and receipt_status = 1
            -- and date(block_timestamp) <= '2021-08-09'
            and date(block_timestamp) = date_sub(@run_date, interval 1 day)
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
            FROM 
                `bigquery-public-data.crypto_ethereum.token_transfers` 
            WHERE true 
                -- and DATE(block_timestamp) <= '2021-08-09'
                and date(block_timestamp) = date_sub(@run_date, interval 1 day)
        ) a 
    inner join 
        (
            select 
                `hash`
            from 
                `bigquery-public-data.crypto_ethereum.transactions`
            WHERE starts_with(input, '0xa9059cbb')
                -- and DATE(block_timestamp) <= '2021-08-09'
                and date(block_timestamp) = date_sub(@run_date, interval 1 day)
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
        -- and price_day <= '2021-08-09'
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
        value * pow(10, -18)  as value, 
        'eth' as symbol
    FROM 
        `bigquery-public-data.crypto_ethereum.traces` 
    WHERE true
        -- and DATE(block_timestamp) <= '2021-08-09'
        and DATE(block_timestamp) = date_sub(@run_date, interval 1 day)
        and value > 0
    --     and from_address is not null -- miner rewards
        and to_address is not null -- smart contract creation but failed.
)
,
valid_tokens as 
(
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
    select * from eth_transfers 
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
addr_in_tx_10k_usd as (
    select distinct 
        to_address as address,
        token_address,
        symbol,
        'addr_in_tx_10k_usd' as feature,
        'P401' as plots,
        1 as idx
    from
        eth_token_transfers_price
    where true
        and to_address is not null
        and usd_value >= 10000
)
,
addr_in_tx_50k_usd as (
    select distinct 
        to_address as address,
        token_address,
        symbol,
        'addr_in_tx_10k_usd' as feature,
        'P401' as plots,
        2 as idx
    from
        eth_token_transfers_price
    where true
        and to_address is not null
        and usd_value >= 50000
)
,
addr_in_tx_100k_usd as (
    select distinct 
        to_address as address,
        token_address,
        symbol,
        'addr_in_tx_10k_usd' as feature,
        'P401' as plots,
        3 as idx
    from
        eth_token_transfers_price
    where true
        and to_address is not null
        and usd_value >= 100000
)
,
addr_out_tx_10k_usd as (
    select distinct 
        to_address as address,
        token_address,
        symbol,
        'addr_out_tx_10k_usd' as feature,
        'P401' as plots,
        4 as idx
    from
        eth_token_transfers_price
    where true
        and to_address is not null
        and usd_value >= 10000
)
,
addr_out_tx_50k_usd as (
    select distinct 
        to_address as address,
        token_address,
        symbol,
        'addr_out_tx_10k_usd' as feature,
        'P401' as plots,
        5 as idx
    from
        eth_token_transfers_price
    where true
        and to_address is not null
        and usd_value >= 50000
)
,
addr_out_tx_100k_usd as (
    select distinct 
        to_address as address,
        token_address,
        symbol,
        'addr_out_tx_10k_usd' as feature,
        'P401' as plots,
        6 as idx
    from
        eth_token_transfers_price
    where true
        and to_address is not null
        and usd_value >= 100000
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
-- select * from res order by address
select 
    a.address as address,
    a.token_address as token_address,
    a.symbol as symbol,
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
            `chaingraph-318604.ethereum_tags.address_large_value_tx`
    ) b
    on a.address = b.address
)
where b.feature is null
;