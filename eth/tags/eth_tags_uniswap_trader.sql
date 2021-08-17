-- truncate table `chaingraph-318604.ethereum_tags.uniswap_trader`;
insert into `chaingraph-318604.ethereum_tags.uniswap_trader`

with
new_traders as
(
    SELECT
        from_address as address,
        min(block_timestamp) as tx_timestamp,
        'uniswap_trader' as tag
    FROM `bigquery-public-data.crypto_ethereum.transactions` 
    WHERE true
    and DATE(block_timestamp) = DATE_ADD(date(current_timestamp()), INTERVAL -1 DAY) 
    and 
    (
        -- uniswap v1
        starts_with(input, '0xf39b5b9b')
        or starts_with(input, '0x6b1d4db7')
        or starts_with(input, '0xad65d76d')
        or starts_with(input, '0x0b573638')
        
        or starts_with(input, '0xddf7e1a7')
        or starts_with(input, '0xb040d545')
        or starts_with(input, '0xf552d91b')
        or starts_with(input, '0xf3c0efe9')
        
        or starts_with(input, '0x013efd8b')
        or starts_with(input, '0x95e3c50b')
        or starts_with(input, '0x7237e031')
        or starts_with(input, '0xd4e4841d')
        
        -- uniswap v2
        or 
        (
            (
                to_address = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'    # uniswap v2 router2
                or to_address = '0xf164fC0Ec4E93095b804a4795bBe1e041497b92a' # uniswap v2 router
            )
            and
            (
                starts_with(input, '0x7ff36ab5')
                or starts_with(input, '0xfb3bdb41')
                or starts_with(input, '0xb6f9de95')
                
                or starts_with(input, '0x8803dbee')
                or starts_with(input, '0x38ed1739')
                or starts_with(input, '0x5c11d795')
                
                or starts_with(input, '0x18cbafe5')
                or starts_with(input, '0x4a25d94a')
                or starts_with(input, '0x791ac947')
            )
        )
        
        -- uniswap v3
        or
        (     
            to_address = '0x7a250d5630b4cf539739df2c5dacb4c659f2488d'
            and
            (
                starts_with(input, '0xc04b8d59')
                or starts_with(input, '0x414bf389')
                or starts_with(input, '0xdb3e2198')
                or starts_with(input, '0xf28c0498')

                -- uniswap v3 multi_call
                or starts_with(input, '0xac9650d8')
            )
        )
    )
    group by from_address
)

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
    from `chaingraph-318604.ethereum_tags.uniswap_trader` a
    right join
    new_traders b on a.address = b.address
) where old_address is null
