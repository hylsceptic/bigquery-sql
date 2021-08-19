declare excute_day date;
set excute_day = date(current_timestamp());

-- truncate table `chaingraph-318604.ethereum_tags.curve_trader`;
insert into `chaingraph-318604.ethereum_tags.curve_trader`

with 
new_traders as
(
    select distinct 
        from_address as address
    from `bigquery-public-data.crypto_ethereum.transactions` a 
    join
    (
        select 
            transaction_hash, 
        from `bigquery-public-data.crypto_ethereum.logs` 
        WHERE true
        and DATE(block_timestamp) = date_sub(excute_day, INTERVAL 1 DAY)
        and ARRAY_LENGTH(topics) > 0 and 
        (
            topics[offset(0)] = '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140'
            or topics[offset(0)] = '0x0bcc4c97732e47d9946f229edb95f5b6323f601300e4690de719993f3c371129'
        )

    ) b 
    on a.`hash` = b.transaction_hash 
    WHERE DATE(a.block_timestamp) = date_sub(excute_day, INTERVAL 1 DAY)
)

select 
    address,
from 
(
    select
        a.address as old_address,
        b.address as address,
    from `chaingraph-318604.ethereum_tags.curve_trader` a
    right join
    new_traders b on a.address = b.address
) where old_address is null

