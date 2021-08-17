# delete from `chaingraph-318604.ethereum_tags.yearn_user` where 1=1
# insert into `chaingraph-318604.ethereum_tags.yearn_user`
-- create or replace table `chaingraph-318604.ethereum_tags.yearn_user` as
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
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
voteAgainst as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_yearn.YearnGovernance_call_voteAgainst`
    where true
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
voteFor as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_yearn.YearnGovernance_call_voteFor`
    where true
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
OwnershipTransferred as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_yearn.YearnGovernance_event_OwnershipTransferred`
    where true
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
RewardAdded as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_yearn.YearnGovernance_event_RewardAdded`
    where true
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
RewardPaid as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_yearn.YearnGovernance_event_RewardPaid`
    where true
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
Staked as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_yearn.YearnGovernance_event_Staked`
    where true
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
Withdrawn as 
(
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_yearn.YearnGovernance_event_Withdrawn`
    where true
        -- and DATE(block_timestamp) <= '2021-08-12'
        and DATE(block_timestamp) = DATE_SUB(@run_date, INTERVAL 1 DAY)
)
,
tmp_user as
(
    select * from voteAgainst 
    union distinct 
    select * from voteFor 
    union distinct 
    select * from OwnershipTransferred 
    union distinct 
    select * from RewardAdded 
    union distinct 
    select * from RewardPaid 
    union distinct 
    select * from Staked 
    union distinct 
    select * from Withdrawn 
)
,
tmp_addr_ts_tag as
(
    select distinct
        a.from_address as address,
        min(a.block_timestamp) as tx_timestamp,
        'Yearn user' as tag
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
        `chaingraph-318604.ethereum_tags.yearn_user` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
-- select distinct * from tmp_addr_ts_tag
;