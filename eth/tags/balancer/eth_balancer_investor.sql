# delete from `chaingraph-318604.ethereum_tags.balancer_investor` where 1=1
# insert into `chaingraph-318604.ethereum_tags.balancer_investor`
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
tmp_v1_investor as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_balancer.BPool_event_LOG_JOIN`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_v2_investor as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_balancer.V2_Vault_event_PoolBalanceChanged`
    where true
        #and DATE(block_timestamp) <= '2021-07-18'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_investors as
(
    select * from tmp_v1_investor 
    union distinct 
    select * from tmp_v2_investor 
)
,
tmp_addr_ts_tag as
(
    select 
        a.from_address as address,
        min(a.block_timestamp) as tx_timestamp,
        'Balancer investor' as tag
    from
    (
        tx_subset a
        join
        tmp_investors b
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
        `chaingraph-318604.ethereum_tags.balancer_investor` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
# select distinct * from tmp_addr_ts_tag
;