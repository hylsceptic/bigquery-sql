# delete from  chaingraph-318604.ethereum_tags.uniswap_pools  where 1=1 

insert into chaingraph-318604.ethereum_tags.uniswap_pools

select 
    address,
    tx_timestamp,
    tag
from
(
    select
        a.address as old_address,
        b.address as address,
        b.tx_timestamp,
        b.tag
    from
    (
        select * from chaingraph-318604.ethereum_tags.uniswap_pools
    ) a
    right join
    (
    SELECT
        case  topics[safe_offset(0)]
        when '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' then concat('0x', substr(data, 27 + 64, 40))
        when '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' then concat('0x', substr(data, 27, 40))
        end as address,
        block_timestamp as tx_timestamp,
        case  topics[safe_offset(0)]
        when '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' then 'uniswap_v3_pool'
        when '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' then 'uniswap_v2(1)_pool'
        end as tag
    FROM `bigquery-public-data.crypto_ethereum.logs`
    WHERE DATE(block_timestamp) >= "2018-10-10"   # for full pools, set date to before Nov-01-2018.
    and (topics[safe_offset(0)] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
    or topics[safe_offset(0)] = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9')
    ) b on a.address = b.address
) where old_address is null