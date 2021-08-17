# insert into `chaingraph-318604.ethereum_tags.tornado_depositor`
with tmp_addr_ts_tag as
(
SELECT 
    from_address as address,
    min(block_timestamp) as tx_timestamp,
    'Tornado depositor' as tag
FROM `bigquery-public-data.crypto_ethereum.transactions` 
WHERE TRUE 
    and DATE(block_timestamp) = DATE_ADD(@run_date, INTERVAL -1 DAY)
    # AND DATE(block_timestamp) >= '2021-07-10'
    and to_address = '0x722122df12d4e14e13ac3b6895a86e84145b6967'
    and starts_with(input, '0x13d98d13') # deposit method
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
        `chaingraph-318604.ethereum_tags.tornado_depositor` b
        on a.address = b.address
    )
)
where address_b is null

# -------------------------- #
# Get historic data
# -------------------------- #
# select distinct * from tmp_addr_ts_tag
;