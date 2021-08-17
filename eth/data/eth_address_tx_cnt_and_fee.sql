create or replace table `chaingraph-318604.ethereum_data.address_tx_cnt_and_fee` as
with 
tx as (
    select
        from_address,
        to_address,
        receipt_effective_gas_price,
        receipt_gas_used,
        -- value * pow(10, -18) as value
    from 
        `bigquery-public-data.crypto_ethereum.transactions` 
    where true 
        -- and date(block_timestamp) <= '2021-08-08'
        and date(block_timestamp) = date_sub(@run_date, interval 1 day)
)
,
out_tx_fee as (
    select
        from_address as address,
        sum(pow(10, -18) * receipt_effective_gas_price * receipt_gas_used) as total_fee
    from 
        tx
    group by address
)
,
tx_cnt as (
    select 
        address,
        count(1) as total_tx_cnt
    from
        (
            select from_address as address from tx
            union all
            select to_address  as address from tx
        )
    where address is not null
    group by address
)
,
res as (
    select distinct 
        address,
        total_tx_cnt,
        coalesce(total_fee, 0) as total_fee
    from
    (
        tx_cnt
        left join 
        out_tx_fee 
        using (address)
    )
)
-- select * from res
select 
    address,
    coalesce(a.total_tx_cnt, 0) + coalesce(b.total_tx_cnt, 0) as total_tx_cnt,
    coalesce(a.total_fee) + coalesce(b.total_fee) as total_fee
from
    `chaingraph-318604.ethereum_data.address_tx_cnt_and_fee` a
    full outer join 
    res b
    using (address)
;