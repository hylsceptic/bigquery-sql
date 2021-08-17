# DECLARE spec_date DATE DEFAULT DATE_ADD(@run_date, INTERVAL -4 DAY);
# DECLARE spec_date DATE DEFAULT '2021-07-01';
# delete   FROM `chaingraph-318604.ethereum_tags.addr_eth2` where 1=1

# DROP TABLE IF EXISTS chaingraph-318604.ethereum_metrics.tx_daily;
# CREATE TABLE chaingraph-318604.ethereum_metrics.tx_daily AS
# INSERT INTO `chaingraph-318604.ethereum_tags.addr_eth2`
with transactions_subset as
(
    select 
        block_timestamp,
        `hash` as transaction_hash,
        from_address
    from
        `bigquery-public-data.crypto_ethereum.transactions`
    where true
        # and block_number >= 11182202
        # and DATE(block_timestamp) = '2021-07-15'
        # and DATE(block_timestamp) >= '2021-07-01' and DATE(block_timestamp) <= '2021-07-10'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_deposit_tx as
(   
    select 
        transaction_hash
    from
        `blockchain-etl.ethereum_eth2.DepositContract_event_DepositEvent`
    WHERE TRUE
        # AND DATE(block_timestamp) = '2021-07-15'
        # and DATE(block_timestamp) >= '2021-07-01' and DATE(block_timestamp) <= '2021-07-10'
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
)
,
tmp_addr_ts_tag as
(
    select 
        a.from_address as address,
        min(a.block_timestamp) as tx_timestamp,
        'ETH2.0 depositor' as tag
    from
    (
        transactions_subset a
        join
        tmp_deposit_tx b
        on a.transaction_hash = b.transaction_hash
    )
    group by address
)
# select * from tmp_addr_ts_tag  # Get historic data
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
        `chaingraph-318604.ethereum_tags.addr_eth2` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Method 2
# -------------------------- #
# select
#     address,
#     coalesce(a.tx_timestamp, b.tx_timestamp) as tx_timestamp,
#     coalesce(a.tag, b.tag) as tag
# from
# (
#     tmp_addr_ts_tag a
#     full outer join 
#     `chaingraph-318604.ethereum_tags.addr_eth2` b
#     using (address)
# )
