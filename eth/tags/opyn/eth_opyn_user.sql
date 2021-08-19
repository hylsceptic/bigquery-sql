# delete from `chaingraph-318604.ethereum_tags.opyn_user` where 1=1
-- insert into `chaingraph-318604.ethereum_tags.opyn_user`
-- create or replace table `chaingraph-318604.ethereum_tags.opyn_user` as
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
        -- and DATE(block_timestamp) <= '2021-08-16' and date(block_timestamp) >= '2021-08-12'
        -- and DATE(block_timestamp) <= '2021-08-11'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
VaultOpened as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_opyn.oToken_event_VaultOpened`
    where true
        -- and DATE(block_timestamp) <= '2021-08-16' and date(block_timestamp) >= '2021-08-12'
        -- and DATE(block_timestamp) <= '2021-08-11'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
Transfer as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_opyn.oToken_event_Transfer`
    where true
        -- and DATE(block_timestamp) <= '2021-08-16' and date(block_timestamp) >= '2021-08-12'
        -- and DATE(block_timestamp) <= '2021-08-11'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
Exercise as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_opyn.oToken_event_Exercise`
    where true
        -- and DATE(block_timestamp) <= '2021-08-16' and date(block_timestamp) >= '2021-08-12'
        -- and DATE(block_timestamp) <= '2021-08-11'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
tmp_user as
(
    select * from VaultOpened 
    union distinct 
    select * from Transfer 
    union distinct 
    select * from Exercise 
)
,
tmp_addr_ts_tag as
(
    select distinct
        a.from_address as address,
        min(a.block_timestamp) as tx_timestamp,
        'Opyn user' as tag
    from
    (
        tx_subset a
        join
        tmp_user b
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
        `chaingraph-318604.ethereum_tags.opyn_user` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
-- select distinct * from tmp_addr_ts_tag
;