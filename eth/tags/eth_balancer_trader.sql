# delete from `chaingraph-318604.ethereum_tags.balancer_trader` where 1=1
# insert into `chaingraph-318604.ethereum_tags.balancer_trader`
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
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_v1_trader as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_balancer.BPool_event_LOG_SWAP`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_v2_trader as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_balancer.V2_Vault_event_Swap`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_traders as
(
    select * from tmp_v1_trader 
    union distinct 
    select * from tmp_v2_trader 
)
,
tmp_addr_ts_tag as
(
    select 
        a.from_address as address,
        min(a.block_timestamp) as tx_timestamp,
        'Balancer trader' as tag
    from
    (
        tx_subset a
        join
        tmp_traders b
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
        `chaingraph-318604.ethereum_tags.balancer_trader` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
# select distinct * from tmp_addr_ts_tag
;