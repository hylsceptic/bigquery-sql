# delete from `chaingraph-318604.ethereum_tags.contract_deployer` where 1=1
# insert into `chaingraph-318604.ethereum_tags.contract_deployer`
with
tmp_addr_ts_tag as
(
    select
        from_address  as address,
        min(block_timestamp) as tx_timestamp,
        'Contract deployer' as tag
    from 
        `bigquery-public-data.crypto_ethereum.transactions` 
    WHERE true
        and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
        # and DATE(block_timestamp) = "2021-07-19" 
        # and receipt_status = 1
        and to_address is null
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
        `chaingraph-318604.ethereum_tags.contract_deployer` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
# select distinct * from tmp_addr_ts_tag
;