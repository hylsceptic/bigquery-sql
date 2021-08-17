# delete from `chaingraph-318604.ethereum_tags.dydx_user` where 1=1
# insert into `chaingraph-318604.ethereum_tags.dydx_user`
with
tx_subset as
(
    select 
        block_timestamp,
        `hash` as transaction_hash,
        from_address
    from
        `bigquery-public-data.crypto_ethereum.transactions`
    where true
        # and DATE(block_timestamp) <= '2021-07-20'
        # and DATE(block_timestamp) >= '2018-09-28'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_solomargin_depositor as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_dydx.SoloMargin_event_LogDeposit`
    where true
        # and DATE(block_timestamp) <= '2021-07-20'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_perpetual_depositor as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_dydx.PerpetualV1_event_LogDeposit`
    where true
        # and DATE(block_timestamp) <= '2021-07-20'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_users as
(
    select * from tmp_solomargin_depositor 
    union distinct 
    select * from tmp_perpetual_depositor 
)
,
tmp_addr_ts_tag as
(
    select 
        a.from_address as address,
        min(a.block_timestamp) as tx_timestamp,
        'Dydx user' as tag
    from
    (
        tx_subset a
        join
        tmp_users b
        on a.transaction_hash = b.transaction_hash
    )
    group by address
)
select
    address_a as address,
    tx_timestamp,
    tag
from
(
    select
        a.address as address_a,
        b.address as address_b,
        a.tx_timestamp as tx_timestamp,
        a.tag as tag
    from 
    (
        tmp_addr_ts_tag a
        left join 
        `chaingraph-318604.ethereum_tags.dydx_user` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
# select distinct * from tmp_addr_ts_tag
;