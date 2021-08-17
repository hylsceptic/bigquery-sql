-- create or replace table `chaingraph-318604.ethereum_tags.address_high_fee` as
with 
tx as (
    select 
        from_address,
        to_address,
        block_number,
        receipt_effective_gas_price
    from
        `bigquery-public-data.crypto_ethereum.transactions`
    where true 
        -- and block_timestamp <= '2021-08-05'
        and date(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
avg_fee as (
    select 
        block_number,
        gas_price_prev6_blk
    from
        `chaingraph-318604.ethereum_data.avg_gas_price`
    where true 
        -- and block_timestamp <= '2021-08-05'
        and date(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
tx_fee as (
    select 
        *
    from
    (
        tx
        join 
        avg_fee
        using (block_number)
    )
)
,
addr_high_fee_in_tx as (
    select distinct 
        to_address as address,
        'address_high_fee_in_tx' as feature,
        'P121' as plots,
        11 as idx,
    from 
        tx_fee
    where true
        and receipt_effective_gas_price >= 5 * gas_price_prev6_blk
)
,
addr_high_fee_out_tx as (
    select distinct 
        from_address as address,
        'address_high_fee_out_tx' as feature,
        'P121' as plots,
        10 as idx,
    from 
        tx_fee
    where true
        and receipt_effective_gas_price >= 5 * gas_price_prev6_blk
)
,
addr_high_fee_tx as (
    select 
        *
    from 
    (
        select * from addr_high_fee_in_tx
        union distinct
        select * from addr_high_fee_out_tx
    )
)
-- select * from addr_high_fee_tx
select 
    a.address as address,
    a.feature as feature,
    null as name,
    a.plots as plots,
    a.idx as idx
from
(
    addr_high_fee_tx a
    left join 
    (
        select 
            * 
        from 
            `chaingraph-318604.ethereum_tags.address_high_fee`
    ) b
    on a.address = b.address
)
where b.feature is null